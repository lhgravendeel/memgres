package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
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
 * <p>A statement, asked what it leaves behind when it fails, answered for the relation it was
 * written against and for no other. Everything it set going on the way is part of it: what a
 * trigger wrote to a second relation, what a rule that trigger fired wrote to a third, what a
 * function the trigger called wrote anywhere. Outside a transaction block a statement is a
 * transaction of its own, so all of it goes when the statement is refused, and all of it stands
 * when the statement succeeds; inside one, the same writes go with ROLLBACK, with ROLLBACK TO
 * SAVEPOINT and with a PL/pgSQL handler that catches the error and carries on.
 *
 * <p>A partition key was asked what may stand in it. A generated column may not: its value is
 * worked out after the row has been routed, so PostgreSQL refuses it outright. And a copy of a
 * partitioned table's row trigger may not carry the row out of the partition the insert was routed
 * to -- where PostgreSQL decides whether the copy rewrote the row by whether the routine handed
 * back a tuple other than the one it was given, so a PL/pgSQL routine that assigns a column its
 * own value has rewritten it while one that only returns NEW has not.
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
        // Since PostgreSQL 15 the public schema grants CREATE to nobody, and memgres now says
        // so too. These tests are about something else, so the grant is made once here rather
        // than in every one of them.
        try (java.sql.Statement grantStmt = conn.createStatement()) {
            grantStmt.execute("GRANT CREATE ON SCHEMA public TO PUBLIC");
        }
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
        // A column that already refuses a null has nothing left for a second name to answer to,
        // and PostgreSQL will not report a constraint it did not make: the hierarchy is left
        // holding the one rule already made, under the one name it was made with.
        assertEquals("55000",
                stateOf("ALTER TABLE ONLY zzy9nn_wp ADD CONSTRAINT zzy9nn_wn2 NOT NULL i"));
        assertEquals("cannot create not-null constraint \"zzy9nn_wn2\" on column \"i\""
                        + " of table \"zzy9nn_wp\"",
                messageOf("ALTER TABLE ONLY zzy9nn_wp ADD CONSTRAINT zzy9nn_wn2 NOT NULL i"));
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

    // ------------------------------------------------------------ The type a default has is the type of the whole expression

    /** The type format_type spells for a relation's one column. */
    private static String typeOfColumn(String relation) throws SQLException {
        return scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = '" + relation + "'::regclass AND attnum > 0"
                + " AND NOT attisdropped");
    }

    /** relname/convalidated for every relation holding a constraint of that name. */
    private static String validatedAcross(String constraint) throws SQLException {
        return scalar("SELECT string_agg(cl.relname||'/'||c.convalidated::text, ','"
                + " ORDER BY cl.relname) FROM pg_constraint c"
                + " JOIN pg_class cl ON cl.oid = c.conrelid"
                + " WHERE c.conname = '" + constraint + "'");
    }

    @Test
    void aDefaultIsMeasuredAgainstTheTypeOfTheWholeExpression() throws Exception {
        // PostgreSQL coerces a DEFAULT to the column's type in assignment context: a cast pg_cast
        // records as implicit or assignment, or -- with no cast row at all -- a read through the
        // value's own text form, which it allows only into a string type. So an operator's result,
        // a cast's target and a call's return type each decide the answer, not only a literal's.
        assertEquals("42804", stateOf("CREATE TABLE zzgd7_c1 (b int DEFAULT 'a'||'b')"));
        assertEquals("column \"b\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzgd7_c1 (b int DEFAULT 'a'||'b')"));
        assertEquals("You will need to rewrite or cast the expression.",
                hintOf("CREATE TABLE zzgd7_c1 (b int DEFAULT 'a'||'b')"));

        assertEquals("column \"b\" is of type integer but default expression is of type integer[]",
                messageOf("CREATE TABLE zzgd7_c2 (b int DEFAULT '{1,2}'::int[])"));
        assertEquals("column \"b\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzgd7_c3 (b int DEFAULT coalesce('a','b'))"));
        assertEquals("column \"b\" is of type interval but default expression is of type"
                        + " timestamp with time zone",
                messageOf("CREATE TABLE zzgd7_c4 (b interval DEFAULT now())"));
        assertEquals("column \"b\" is of type boolean but default expression is of type text",
                messageOf("CREATE TABLE zzgd7_c5 (b boolean DEFAULT greatest('a','b'))"));
        assertEquals("column \"b\" is of type date but default expression is of type integer",
                messageOf("CREATE TABLE zzgd7_c6 (b date DEFAULT 1)"));
        assertEquals("column \"a\" is of type integer but default expression is of type boolean",
                messageOf("CREATE TABLE zzgd7_c7 (a int DEFAULT true)"));
        assertEquals("column \"a\" is of type boolean but default expression is of type integer",
                messageOf("CREATE TABLE zzgd7_c8 (a boolean DEFAULT 1)"));

        // None of them left a relation behind.
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname IN ('zzgd7_c1',"
                + " 'zzgd7_c2','zzgd7_c3','zzgd7_c4','zzgd7_c5','zzgd7_c6','zzgd7_c7','zzgd7_c8')"));
    }

    @Test
    void everyAssignmentCastPostgresHasIsStillTakenForADefault() throws Exception {
        // The assignment casts PostgreSQL does have, and the general rule beside them that a
        // string type takes any value at all through its own output function.
        exec("CREATE TABLE zzgd7_ok (a bigint DEFAULT 1, b text DEFAULT 1, c numeric DEFAULT 1,"
                + " d text DEFAULT now(), e varchar(4) DEFAULT 1,"
                + " f timestamp DEFAULT current_date, g date DEFAULT now())");
        exec("INSERT INTO zzgd7_ok DEFAULT VALUES");
        assertEquals("1/1/1", rowsOf("SELECT a, b, c FROM zzgd7_ok"));
        assertEquals(7, num("SELECT count(*)::int FROM information_schema.columns"
                + " WHERE table_name = 'zzgd7_ok'"));
        exec("DROP TABLE zzgd7_ok");

        exec("CREATE TABLE zzgd7_ok3 (a int DEFAULT 1.7, b smallint DEFAULT 3, c real DEFAULT 1,"
                + " d char(3) DEFAULT 42, e text DEFAULT current_date,"
                + " f bigint DEFAULT 2::smallint, g numeric(4,1) DEFAULT 2, h text DEFAULT 1+1,"
                + " i varchar(30) DEFAULT now()::date, j double precision DEFAULT 1)");
        exec("INSERT INTO zzgd7_ok3 DEFAULT VALUES");
        // A numeric reaching an integer column is rounded, and a number reaches a character type
        // through its own text form, blank-padded where the type is.
        assertEquals("2/3/1/42 /2/2.0/2/1",
                rowsOf("SELECT a, b, c, d, f, g, h, j FROM zzgd7_ok3"));
        exec("DROP TABLE zzgd7_ok3");
    }

    @Test
    void aDefaultOfNoTypeOfItsOwnIsReadByTheColumnsInputFunction() throws Exception {
        exec("CREATE TABLE zzgd7_v (a int DEFAULT 1::bigint, b date DEFAULT '2020-01-01',"
                + " c interval DEFAULT '1 day', d int DEFAULT '5', e boolean DEFAULT 'yes',"
                + " f int[] DEFAULT '{1,2}', g int DEFAULT NULL, h numeric DEFAULT 1.5,"
                + " i text DEFAULT 3.5)");
        exec("INSERT INTO zzgd7_v DEFAULT VALUES");
        assertEquals("1/2020-01-01/1 day/5/t/{1,2}/null/1.5/3.5",
                rowsOf("SELECT a, b, c, d, e, f, g, h, i FROM zzgd7_v"));
        exec("DROP TABLE zzgd7_v");

        // A literal the column's input function cannot read is refused as a value, not as a type.
        assertEquals("22P02", stateOf("CREATE TABLE zzgd7_v2 (a int DEFAULT 'x')"));
        assertEquals("invalid input syntax for type integer: \"x\"",
                messageOf("CREATE TABLE zzgd7_v2 (a int DEFAULT 'x')"));

        // A default too long for the column is a question about the value, so it is asked when
        // the row is written and not when the column is declared.
        exec("CREATE TABLE zzgd7_vv (j varchar(5) DEFAULT 1000000)");
        assertEquals("22001", stateOf("INSERT INTO zzgd7_vv DEFAULT VALUES"));
        assertEquals("value too long for type character varying(5)",
                messageOf("INSERT INTO zzgd7_vv DEFAULT VALUES"));
        assertEquals(0, num("SELECT count(*)::int FROM zzgd7_vv"));
        exec("DROP TABLE zzgd7_vv");
    }

    @Test
    void theSameRuleWhereTheColumnIsAddedAndWhereTheDefaultIsSetLater() throws Exception {
        exec("CREATE TABLE zzgd7_a (k int, b1 int, b2 interval)");

        assertEquals("42804", stateOf("ALTER TABLE zzgd7_a ADD COLUMN a1 int DEFAULT 'a'||'b'"));
        assertEquals("column \"a1\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzgd7_a ADD COLUMN a1 int DEFAULT 'a'||'b'"));
        assertEquals("You will need to rewrite or cast the expression.",
                hintOf("ALTER TABLE zzgd7_a ADD COLUMN a1 int DEFAULT 'a'||'b'"));
        assertEquals("column \"a2\" is of type integer but default expression is of type integer[]",
                messageOf("ALTER TABLE zzgd7_a ADD COLUMN a2 int DEFAULT '{1,2}'::int[]"));
        assertEquals("column \"a3\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzgd7_a ADD COLUMN a3 int DEFAULT coalesce('a','b')"));
        assertEquals("column \"a4\" is of type interval but default expression is of type"
                        + " timestamp with time zone",
                messageOf("ALTER TABLE zzgd7_a ADD COLUMN a4 interval DEFAULT now()"));

        assertEquals("column \"b1\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzgd7_a ALTER COLUMN b1 SET DEFAULT 'a'||'b'"));
        assertEquals("column \"b1\" is of type integer but default expression is of type integer[]",
                messageOf("ALTER TABLE zzgd7_a ALTER COLUMN b1 SET DEFAULT '{1,2}'::int[]"));
        assertEquals("column \"b2\" is of type interval but default expression is of type"
                        + " timestamp with time zone",
                messageOf("ALTER TABLE zzgd7_a ALTER COLUMN b2 SET DEFAULT now()"));

        // What PostgreSQL takes still stands, and no column a refusal turned away was added.
        exec("ALTER TABLE zzgd7_a ALTER COLUMN b1 SET DEFAULT 7");
        exec("ALTER TABLE zzgd7_a ADD COLUMN a5 bigint DEFAULT 1");
        exec("ALTER TABLE zzgd7_a ADD COLUMN a6 text DEFAULT 1");
        exec("INSERT INTO zzgd7_a (k) VALUES (1)");
        assertEquals("1/7/1/1", rowsOf("SELECT k, b1, a5, a6 FROM zzgd7_a"));
        assertEquals("k,b1,b2,a5,a6",
                column("SELECT attname FROM pg_attribute WHERE attrelid = 'zzgd7_a'::regclass"
                        + " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"));

        exec("DROP TABLE zzgd7_a");
    }

    @Test
    void aGenerationExpressionIsMeasuredAgainstItsColumnInTheSameWords() throws Exception {
        // It is the same code that stores a default and a generation expression, so PostgreSQL
        // words the refusal the same way for both.
        exec("CREATE TABLE zzgd7_ga (k int)");
        assertEquals("42804", stateOf("ALTER TABLE zzgd7_ga ADD COLUMN g1 int"
                + " GENERATED ALWAYS AS ('a'||'b') STORED"));
        assertEquals("column \"g1\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzgd7_ga ADD COLUMN g1 int"
                        + " GENERATED ALWAYS AS ('a'||'b') STORED"));
        assertEquals("column \"g\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzgd7_g (a int,"
                        + " g int GENERATED ALWAYS AS ('a'||'b') STORED)"));
        exec("DROP TABLE zzgd7_ga");

        // A generation expression PostgreSQL can coerce is taken, and the column holds what the
        // coercion made of it.
        exec("CREATE TABLE zzgd7_gv (a int, g bigint GENERATED ALWAYS AS (a * 2) STORED,"
                + " h text GENERATED ALWAYS AS (a) STORED,"
                + " i numeric GENERATED ALWAYS AS (a) STORED)");
        exec("INSERT INTO zzgd7_gv (a) VALUES (4)");
        assertEquals("4/8/4/4", rowsOf("SELECT a, g, h, i FROM zzgd7_gv"));
        exec("DROP TABLE zzgd7_gv");
    }

    // ------------------------------------------------------------ The names a stored expression may use

    @Test
    void aStoredExpressionMayNameTheRelationBeingDefined() throws Exception {
        // PostgreSQL stores a reference to the relation rather than the name that was written, so
        // the qualifier is judged where it is written and never reaches the stored tree.
        exec("CREATE TABLE zzgd7_q (a int, b int GENERATED ALWAYS AS (zzgd7_q.a * 2) STORED)");
        exec("INSERT INTO zzgd7_q (a) VALUES (5)");
        assertEquals("5/10", rowsOf("SELECT a, b FROM zzgd7_q"));

        // The same name reaches the relation in a CHECK, in an index key, in an index predicate
        // and in a USING clause.
        exec("CREATE TABLE zzgd7_r (a int, nosuch int, CHECK (zzgd7_r.a > 0))");
        exec("CREATE INDEX zzgd7_r_i ON zzgd7_r (a) WHERE zzgd7_r.a > 0");
        exec("CREATE INDEX zzgd7_r_j ON zzgd7_r ((zzgd7_r.a + 1))");
        exec("ALTER TABLE zzgd7_r ALTER COLUMN a TYPE bigint USING zzgd7_r.a");
        assertEquals("bigint", scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = 'zzgd7_r'::regclass AND attname = 'a'"));
        exec("INSERT INTO zzgd7_r VALUES (3, 4)");
        assertEquals("3/4", rowsOf("SELECT a, nosuch FROM zzgd7_r"));

        exec("DROP TABLE zzgd7_r");
        exec("DROP TABLE zzgd7_q");
    }

    @Test
    void aQualifierNamingAnyOtherRelationIsAMissingFromClauseEntry() throws Exception {
        exec("CREATE TABLE zzgd7_qq (a int)");

        assertEquals("42P01", stateOf("CREATE TABLE zzgd7_q2 (a int,"
                + " b int GENERATED ALWAYS AS (nosuchrel.a) STORED)"));
        assertEquals("missing FROM-clause entry for table \"nosuchrel\"",
                messageOf("CREATE TABLE zzgd7_q2 (a int,"
                        + " b int GENERATED ALWAYS AS (nosuchrel.a) STORED)"));
        // Another relation's name, even one that exists, is not in scope either.
        assertEquals("missing FROM-clause entry for table \"zzgd7_qq\"",
                messageOf("CREATE TABLE zzgd7_q3 (a int,"
                        + " b int GENERATED ALWAYS AS (zzgd7_qq.a) STORED)"));
        // A qualified reference to a generated column is still a generated column.
        assertEquals("42P17", stateOf("CREATE TABLE zzgd7_q4 (a int,"
                + " g int GENERATED ALWAYS AS (zzgd7_q4.g) STORED)"));
        assertEquals("cannot use generated column \"g\" in column generation expression",
                messageOf("CREATE TABLE zzgd7_q4 (a int,"
                        + " g int GENERATED ALWAYS AS (zzgd7_q4.g) STORED)"));
        assertEquals("A generated column cannot reference another generated column.",
                detailOf("CREATE TABLE zzgd7_q4 (a int,"
                        + " g int GENERATED ALWAYS AS (zzgd7_q4.g) STORED)"));

        // And every other place a definition is stored reads the qualifier the same way.
        assertEquals("missing FROM-clause entry for table \"nosuchrel\"",
                messageOf("CREATE INDEX zzgd7_qq_k ON zzgd7_qq (a) WHERE nosuchrel.a > 0"));
        assertEquals("missing FROM-clause entry for table \"nosuchrel\"",
                messageOf("CREATE INDEX zzgd7_qq_l ON zzgd7_qq ((nosuchrel.a + 1))"));
        assertEquals("missing FROM-clause entry for table \"nosuchrel\"",
                messageOf("ALTER TABLE zzgd7_qq ADD CONSTRAINT zzgd7_qq_c"
                        + " CHECK (nosuchrel.a > 0)"));
        assertEquals("missing FROM-clause entry for table \"nosuchrel\"",
                messageOf("ALTER TABLE zzgd7_qq ALTER COLUMN a TYPE int USING nosuchrel.a"));

        assertNull(indexDefsOf("zzgd7_qq"));
        assertNull(constraintsOf("zzgd7_qq"));
        exec("DROP TABLE zzgd7_qq");
    }

    @Test
    void aNameNearlyOneOfTheRelationsOwnIsOfferedInTheHint() throws Exception {
        // The one relation the definition is stored on is the whole of what was in scope, so
        // PostgreSQL offers a column of it spelled almost the same way -- qualified, as it writes
        // every such suggestion.
        exec("CREATE TABLE zzgd7_h (a int, nosuch int)");
        String hint = "Perhaps you meant to reference the column \"zzgd7_h.nosuch\".";

        assertEquals("42703",
                stateOf("ALTER TABLE zzgd7_h ADD CONSTRAINT zzgd7_h_c CHECK (nosuchh > 0)"));
        assertEquals("column \"nosuchh\" does not exist",
                messageOf("ALTER TABLE zzgd7_h ADD CONSTRAINT zzgd7_h_c CHECK (nosuchh > 0)"));
        assertEquals(hint,
                hintOf("ALTER TABLE zzgd7_h ADD CONSTRAINT zzgd7_h_c CHECK (nosuchh > 0)"));
        assertEquals(hint, hintOf("ALTER TABLE zzgd7_h ADD COLUMN gg int"
                + " GENERATED ALWAYS AS (nosuchh) STORED"));
        assertEquals(hint, hintOf("ALTER TABLE zzgd7_h ALTER COLUMN a TYPE int USING nosuchh"));
        assertEquals(hint, hintOf("CREATE INDEX zzgd7_h_i ON zzgd7_h (a) WHERE nosuchh > 0"));
        assertEquals(hint, hintOf("CREATE INDEX zzgd7_h_j ON zzgd7_h ((nosuchh + 1))"));

        // The hint names the relation the statement is defining, even where nothing is stored yet.
        assertEquals("Perhaps you meant to reference the column \"zzgd7_h2.nosuch\".",
                hintOf("CREATE TABLE zzgd7_h2 (a int, nosuch int, CHECK (nosuchh > 0))"));

        assertNull(indexDefsOf("zzgd7_h"));
        exec("DROP TABLE zzgd7_h");
    }

    @Test
    void aDefaultMayNameNoColumnAtAllItsOwnRelationsIncluded() throws Exception {
        assertEquals("0A000", stateOf("CREATE TABLE zzgd7_gt (a int DEFAULT zzgd7_gt.a)"));
        assertEquals("cannot use column reference in DEFAULT expression",
                messageOf("CREATE TABLE zzgd7_gt (a int DEFAULT zzgd7_gt.a)"));
        assertEquals("cannot use column reference in DEFAULT expression",
                messageOf("CREATE TABLE zzgd7_gu (a int, b int DEFAULT nosuchrel.a)"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname IN ('zzgd7_gt','zzgd7_gu')"));
    }

    // ------------------------------------------------------------ A function a stored expression names has to exist when it is written

    @Test
    void everyPlaceAnExpressionIsStoredResolvesTheFunctionsItNames() throws Exception {
        exec("CREATE TABLE zzgd7_f (a int, b text)");
        String missing = "function nosuchfunc(integer) does not exist";

        assertEquals("42883",
                stateOf("ALTER TABLE zzgd7_f ADD CONSTRAINT zzgd7_f_c CHECK (nosuchfunc(a) > 0)"));
        assertEquals(missing,
                messageOf("ALTER TABLE zzgd7_f ADD CONSTRAINT zzgd7_f_c CHECK (nosuchfunc(a) > 0)"));
        assertEquals("No function matches the given name and argument types."
                        + " You might need to add explicit type casts.",
                hintOf("ALTER TABLE zzgd7_f ADD CONSTRAINT zzgd7_f_c CHECK (nosuchfunc(a) > 0)"));

        assertEquals(missing, messageOf("CREATE TABLE zzgd7_f2 (a int,"
                + " CHECK (nosuchfunc(a) > 0))"));
        assertEquals(missing, messageOf("ALTER TABLE zzgd7_f ALTER COLUMN a TYPE int"
                + " USING nosuchfunc(a)"));
        assertEquals(missing, messageOf("ALTER TABLE zzgd7_f ADD COLUMN g int"
                + " GENERATED ALWAYS AS (nosuchfunc(a)) STORED"));
        assertEquals(missing, messageOf("CREATE TABLE zzgd7_f3 (a int,"
                + " g int GENERATED ALWAYS AS (nosuchfunc(a)) STORED)"));
        assertEquals(missing, messageOf("CREATE TABLE zzgd7_f4 (a int DEFAULT nosuchfunc(1))"));
        assertEquals(missing, messageOf("ALTER TABLE zzgd7_f ADD COLUMN h int"
                + " DEFAULT nosuchfunc(1)"));
        assertEquals(missing, messageOf("ALTER TABLE zzgd7_f ALTER COLUMN a"
                + " SET DEFAULT nosuchfunc(1)"));
        assertEquals(missing, messageOf("CREATE INDEX zzgd7_f_i ON zzgd7_f (a)"
                + " WHERE nosuchfunc(a) > 0"));
        assertEquals(missing, messageOf("CREATE INDEX zzgd7_f_j ON zzgd7_f ((nosuchfunc(a)))"));

        assertNull(constraintsOf("zzgd7_f"));
        assertNull(indexDefsOf("zzgd7_f"));
        exec("DROP TABLE zzgd7_f");
    }

    @Test
    void theSignatureTheComplaintNamesIsTheOneItLookedFor() throws Exception {
        // The name is looked up with the argument types that were worked out, so the complaint
        // says which signature it looked for. A literal written with no type of its own is still
        // of type unknown and is named as one.
        exec("CREATE TABLE zzgd7_s (a int, b text)");
        assertEquals("function nosuchfunc(unknown) does not exist",
                messageOf("ALTER TABLE zzgd7_s ADD CONSTRAINT z2 CHECK (nosuchfunc('x') > 0)"));
        assertEquals("function nosuchfunc(integer, unknown) does not exist",
                messageOf("ALTER TABLE zzgd7_s ADD CONSTRAINT z3 CHECK (nosuchfunc(a,'x') > 0)"));
        assertEquals("function nosuchfunc() does not exist",
                messageOf("ALTER TABLE zzgd7_s ADD CONSTRAINT z4 CHECK (nosuchfunc() > 0)"));
        assertEquals("function pg_catalog.nosuchfunc(integer) does not exist",
                messageOf("ALTER TABLE zzgd7_s ADD CONSTRAINT z5"
                        + " CHECK (pg_catalog.nosuchfunc(a) > 0)"));
        exec("DROP TABLE zzgd7_s");
    }

    @Test
    void aCallOfOneArgumentNamingATypeIsReadAsACastWrittenTheOtherWayRound() throws Exception {
        exec("CREATE TABLE zzgd7_d (a int, b text)");
        exec("CREATE DOMAIN zzgd7_dom AS int");

        // It is the one argument that makes it a cast; the same name over two arguments, or none,
        // is a missing function there as anywhere else.
        exec("ALTER TABLE zzgd7_d ADD CONSTRAINT zzgd7_d_d1 CHECK (zzgd7_dom(a) > 0)");
        assertEquals("function zzgd7_dom(integer, text) does not exist",
                messageOf("ALTER TABLE zzgd7_d ADD CONSTRAINT zzgd7_d_d2"
                        + " CHECK (zzgd7_dom(a, b) > 0)"));
        assertEquals("function zzgd7_dom() does not exist",
                messageOf("ALTER TABLE zzgd7_d ADD CONSTRAINT zzgd7_d_d3 CHECK (zzgd7_dom() > 0)"));

        exec("DROP TABLE zzgd7_d");
        exec("DROP DOMAIN zzgd7_dom");
    }

    @Test
    void aFunctionThatExistsIsLeftAloneWhereverADefinitionNamesIt() throws Exception {
        exec("CREATE TABLE zzgd7_e (a int, b text)");
        exec("CREATE FUNCTION zzgd7_fn(int) RETURNS int AS 'SELECT $1' LANGUAGE sql IMMUTABLE");
        exec("ALTER TABLE zzgd7_e ADD CONSTRAINT zzgd7_e_d4 CHECK (zzgd7_fn(a) > 0)");
        exec("CREATE INDEX zzgd7_e_fi ON zzgd7_e ((zzgd7_fn(a)))");
        exec("CREATE INDEX zzgd7_e_k ON zzgd7_e (lower(b))");
        exec("CREATE INDEX zzgd7_e_l ON zzgd7_e (a) WHERE upper(b) > 'A'");
        exec("ALTER TABLE zzgd7_e ADD CONSTRAINT zzgd7_e_ok CHECK (length(b) < 20)");
        exec("ALTER TABLE zzgd7_e ADD COLUMN gg text GENERATED ALWAYS AS (upper(b)) STORED");
        exec("INSERT INTO zzgd7_e (a, b) VALUES (1, 'ab')");

        assertEquals("1/AB", rowsOf("SELECT a, gg FROM zzgd7_e"));
        assertEquals("zzgd7_e_d4,zzgd7_e_ok",
                column("SELECT conname FROM pg_constraint"
                        + " WHERE conrelid = 'zzgd7_e'::regclass ORDER BY 1"));

        exec("DROP TABLE zzgd7_e");
        exec("DROP FUNCTION zzgd7_fn(int)");
    }

    // ------------------------------------------------------------ What a CHECK is refused for follows the order it was written in

    @Test
    void aCheckIsRefusedForTheFirstFaultReadingLeftToRight() throws Exception {
        // PostgreSQL transforms the expression as it walks it, settling every name and every call
        // at the node it stands at, so the same two faults the other way round get the other
        // complaint.
        exec("CREATE TABLE zzgd7_o (a int, nosuch int)");

        assertEquals("42703", stateOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c1"
                + " CHECK (nosuchcol > 0 AND (SELECT true))"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c1"
                        + " CHECK (nosuchcol > 0 AND (SELECT true))"));
        assertEquals("Perhaps you meant to reference the column \"zzgd7_o.nosuch\".",
                hintOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c1"
                        + " CHECK (nosuchcol > 0 AND (SELECT true))"));
        assertEquals("0A000", stateOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c2"
                + " CHECK ((SELECT true) AND nosuchcol > 0)"));
        assertEquals("cannot use subquery in check constraint",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c2"
                        + " CHECK ((SELECT true) AND nosuchcol > 0)"));

        // An aggregate is the same: it is refused where it stands, and a name written before it
        // is reached first.
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c3"
                        + " CHECK (nosuchcol > 0 AND count(a) > 0)"));
        assertEquals("42803", stateOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c4"
                + " CHECK (count(a) > 0 AND nosuchcol > 0)"));
        assertEquals("aggregate functions are not allowed in check constraints",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c4"
                        + " CHECK (count(a) > 0 AND nosuchcol > 0)"));

        // And so is a call naming no function.
        assertEquals("function nosuchfunc(integer) does not exist",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c5"
                        + " CHECK (nosuchfunc(a) > 0 AND (SELECT true))"));
        assertEquals("cannot use subquery in check constraint",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c6"
                        + " CHECK ((SELECT true) AND nosuchfunc(a) > 0)"));
        assertEquals("function nosuchfunc(integer) does not exist",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c7"
                        + " CHECK (nosuchfunc(a) > 0 AND nosuchcol > 0)"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c8"
                        + " CHECK (nosuchcol > 0 AND nosuchfunc(a) > 0)"));
        // A call's arguments are settled before the call itself.
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("ALTER TABLE zzgd7_o ADD CONSTRAINT c9"
                        + " CHECK (nosuchfunc(nosuchcol) > 0)"));

        // The same reading in a CREATE TABLE's own CHECK clause.
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("CREATE TABLE zzgd7_o2 (a int, nosuch int,"
                        + " CHECK (nosuchcol > 0 AND (SELECT true)))"));

        // Nothing that was accepted before is refused now.
        exec("ALTER TABLE zzgd7_o ADD CONSTRAINT zzgd7_o_ok"
                + " CHECK (a > 0 AND nosuch IS NOT NULL)");
        exec("INSERT INTO zzgd7_o VALUES (1, 2)");
        assertEquals("1/2", rowsOf("SELECT a, nosuch FROM zzgd7_o"));
        assertEquals("zzgd7_o_ok",
                column("SELECT conname FROM pg_constraint"
                        + " WHERE conrelid = 'zzgd7_o'::regclass ORDER BY 1"));

        exec("DROP TABLE zzgd7_o");
    }

    @Test
    void aSubqueryIsRefusedInACheckInEveryShapeItHas() throws Exception {
        exec("CREATE TABLE zzgd7_w (a int)");
        assertEquals("cannot use subquery in check constraint",
                messageOf("ALTER TABLE zzgd7_w ADD CONSTRAINT w1 CHECK ((SELECT true))"));
        assertEquals("cannot use subquery in check constraint",
                messageOf("ALTER TABLE zzgd7_w ADD CONSTRAINT w2 CHECK (a IN (SELECT 1))"));
        assertEquals("cannot use subquery in check constraint",
                messageOf("ALTER TABLE zzgd7_w ADD CONSTRAINT w3 CHECK (EXISTS (SELECT 1))"));
        assertEquals("cannot use subquery in check constraint",
                messageOf("CREATE TABLE zzgd7_w2 (a int, CHECK ((SELECT true)))"));
        assertNull(constraintsOf("zzgd7_w"));
        exec("DROP TABLE zzgd7_w");
    }

    // ------------------------------------------------------------ The type a column is changed to is any type, not only a built-in one

    @Test
    void aColumnMayBeChangedToADomainAndReadsBackAsThatDomain() throws Exception {
        exec("CREATE DOMAIN zzgd7_dm AS int");
        exec("CREATE DOMAIN zzgd7_dm2 AS zzgd7_dm");
        exec("CREATE TABLE zzgd7_t1 (s text)");

        exec("ALTER TABLE zzgd7_t1 ALTER COLUMN s TYPE zzgd7_dm USING s::int");
        assertEquals("zzgd7_dm", typeOfColumn("zzgd7_t1"));
        // A domain is transparent for casting, so its base type is reachable again...
        exec("ALTER TABLE zzgd7_t1 ALTER COLUMN s TYPE int");
        assertEquals("integer", typeOfColumn("zzgd7_t1"));
        // ...and an integer column reaches an integer domain with no USING clause, because the
        // domain's base type is what the assignment cast is judged against.
        exec("ALTER TABLE zzgd7_t1 ALTER COLUMN s TYPE zzgd7_dm");
        assertEquals("zzgd7_dm", typeOfColumn("zzgd7_t1"));

        // A domain over a domain is a type of its own, and the column reads back as the one that
        // was named.
        exec("ALTER TABLE zzgd7_t1 ALTER COLUMN s TYPE zzgd7_dm2");
        assertEquals("zzgd7_dm2", typeOfColumn("zzgd7_t1"));
        exec("DROP TABLE zzgd7_t1");

        // A text column does not reach an integer domain on its own, and PostgreSQL puts the
        // conversion the writer meant in the hint.
        exec("CREATE TABLE zzgd7_t4 (s text)");
        assertEquals("42804", stateOf("ALTER TABLE zzgd7_t4 ALTER COLUMN s TYPE zzgd7_dm"));
        assertEquals("column \"s\" cannot be cast automatically to type zzgd7_dm",
                messageOf("ALTER TABLE zzgd7_t4 ALTER COLUMN s TYPE zzgd7_dm"));
        assertEquals("You might need to specify \"USING s::zzgd7_dm\".",
                hintOf("ALTER TABLE zzgd7_t4 ALTER COLUMN s TYPE zzgd7_dm"));
        assertEquals("text", typeOfColumn("zzgd7_t4"));

        exec("DROP TABLE zzgd7_t4");
        exec("DROP DOMAIN zzgd7_dm2");
        exec("DROP DOMAIN zzgd7_dm");
    }

    @Test
    void aColumnMayBeChangedToACompositeThroughAUsingClause() throws Exception {
        exec("CREATE TYPE zzgd7_comp AS (x int, y text)");
        exec("CREATE TYPE zzgd7_comp2 AS (x int)");
        exec("CREATE TABLE zzgd7_c1 (a text)");

        assertEquals("42804", stateOf("ALTER TABLE zzgd7_c1 ALTER COLUMN a TYPE zzgd7_comp"));
        assertEquals("column \"a\" cannot be cast automatically to type zzgd7_comp",
                messageOf("ALTER TABLE zzgd7_c1 ALTER COLUMN a TYPE zzgd7_comp"));
        assertEquals("You might need to specify \"USING a::zzgd7_comp\".",
                hintOf("ALTER TABLE zzgd7_c1 ALTER COLUMN a TYPE zzgd7_comp"));

        exec("ALTER TABLE zzgd7_c1 ALTER COLUMN a TYPE zzgd7_comp USING ROW(1, a)::zzgd7_comp");
        assertEquals("zzgd7_comp", typeOfColumn("zzgd7_c1"));
        // The column is already that composite, so the retype has nothing to convert.
        exec("ALTER TABLE zzgd7_c1 ALTER COLUMN a TYPE zzgd7_comp");
        // Another composite is another type.
        assertEquals("column \"a\" cannot be cast automatically to type zzgd7_comp2",
                messageOf("ALTER TABLE zzgd7_c1 ALTER COLUMN a TYPE zzgd7_comp2"));
        // A composite reaches a string type by an assignment cast, as every type does.
        exec("ALTER TABLE zzgd7_c1 ALTER COLUMN a TYPE varchar(4)");
        assertEquals("character varying(4)", typeOfColumn("zzgd7_c1"));
        exec("DROP TABLE zzgd7_c1");

        // Out of a composite into a number there is no cast at all.
        exec("CREATE TABLE zzgd7_c2 (a zzgd7_comp)");
        assertEquals("42804", stateOf("ALTER TABLE zzgd7_c2 ALTER COLUMN a TYPE int"));
        assertEquals("column \"a\" cannot be cast automatically to type integer",
                messageOf("ALTER TABLE zzgd7_c2 ALTER COLUMN a TYPE int"));
        assertEquals("You might need to specify \"USING a::integer\".",
                hintOf("ALTER TABLE zzgd7_c2 ALTER COLUMN a TYPE int"));
        exec("ALTER TABLE zzgd7_c2 ALTER COLUMN a TYPE text");
        assertEquals("text", typeOfColumn("zzgd7_c2"));

        exec("DROP TABLE zzgd7_c2");
        exec("DROP TYPE zzgd7_comp2");
        exec("DROP TYPE zzgd7_comp");
    }

    @Test
    void oneEnumDoesNotReachAnotherButBothReachAStringType() throws Exception {
        exec("CREATE TYPE zzgd7_en AS ENUM ('a','b')");
        exec("CREATE TYPE zzgd7_en2 AS ENUM ('c')");
        exec("CREATE TABLE zzgd7_e1 (a zzgd7_en)");

        assertEquals("42804", stateOf("ALTER TABLE zzgd7_e1 ALTER COLUMN a TYPE zzgd7_en2"));
        assertEquals("column \"a\" cannot be cast automatically to type zzgd7_en2",
                messageOf("ALTER TABLE zzgd7_e1 ALTER COLUMN a TYPE zzgd7_en2"));
        assertEquals("You might need to specify \"USING a::zzgd7_en2\".",
                hintOf("ALTER TABLE zzgd7_e1 ALTER COLUMN a TYPE zzgd7_en2"));

        // The enum the column already carries is not a conversion at all.
        exec("ALTER TABLE zzgd7_e1 ALTER COLUMN a TYPE zzgd7_en");
        exec("ALTER TABLE zzgd7_e1 ALTER COLUMN a TYPE text");
        assertEquals("text", typeOfColumn("zzgd7_e1"));

        exec("DROP TABLE zzgd7_e1");
        exec("DROP TYPE zzgd7_en2");
        exec("DROP TYPE zzgd7_en");
    }

    @Test
    void aRetypeToADomainKeepsTheDefaultTheConstraintAndTheIndex() throws Exception {
        exec("CREATE DOMAIN zzgd7_pos AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE zzgd7_k (a int DEFAULT 5)");
        exec("CREATE INDEX zzgd7_kx ON zzgd7_k (a)");
        exec("ALTER TABLE zzgd7_k ADD CONSTRAINT zzgd7_kck CHECK (a > 1)");
        exec("INSERT INTO zzgd7_k VALUES (3)");
        exec("ALTER TABLE zzgd7_k ALTER COLUMN a TYPE zzgd7_pos");

        assertEquals("zzgd7_pos", typeOfColumn("zzgd7_k"));
        assertEquals("5", scalar("SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef"
                + " WHERE adrelid = 'zzgd7_k'::regclass"));
        assertEquals("CREATE INDEX zzgd7_kx ON public.zzgd7_k USING btree (a)",
                indexDef("zzgd7_kx"));
        assertEquals("3", scalar("SELECT a FROM zzgd7_k"));

        // The column is a column of the domain from here on, so what is written into it is held
        // to the domain's own rules as well as to the relation's.
        assertEquals("23514", stateOf("INSERT INTO zzgd7_k VALUES (-4)"));
        assertEquals("value for domain zzgd7_pos violates check constraint \"zzgd7_pos_check\"",
                messageOf("INSERT INTO zzgd7_k VALUES (-4)"));
        assertEquals("new row for relation \"zzgd7_k\" violates check constraint \"zzgd7_kck\"",
                messageOf("INSERT INTO zzgd7_k VALUES (1)"));
        assertEquals("3", rowsOf("SELECT a FROM zzgd7_k ORDER BY 1"));
        exec("DROP TABLE zzgd7_k");

        // A row the domain would not have taken stops the retype, and nothing moves.
        exec("CREATE TABLE zzgd7_k2 (a int)");
        exec("INSERT INTO zzgd7_k2 VALUES (-2)");
        assertEquals("23514", stateOf("ALTER TABLE zzgd7_k2 ALTER COLUMN a TYPE zzgd7_pos"));
        assertEquals("value for domain zzgd7_pos violates check constraint \"zzgd7_pos_check\"",
                messageOf("ALTER TABLE zzgd7_k2 ALTER COLUMN a TYPE zzgd7_pos"));
        assertEquals("integer", typeOfColumn("zzgd7_k2"));
        assertEquals("-2", scalar("SELECT a FROM zzgd7_k2"));

        exec("DROP TABLE zzgd7_k2");
        exec("DROP DOMAIN zzgd7_pos");
    }

    @Test
    void aRetypeToADomainReachesEveryDescendantAndTheColumnDependsOnIt() throws Exception {
        exec("CREATE DOMAIN zzgd7_hd AS int");
        exec("CREATE TABLE zzgd7_hp (a text)");
        exec("CREATE TABLE zzgd7_hc () INHERITS (zzgd7_hp)");
        exec("ALTER TABLE zzgd7_hp ALTER COLUMN a TYPE zzgd7_hd USING a::int");

        assertEquals("zzgd7_hc/zzgd7_hd,zzgd7_hp/zzgd7_hd",
                scalar("SELECT string_agg(c.relname||'/'"
                        + "||format_type(a.atttypid, a.atttypmod), ',' ORDER BY c.relname)"
                        + " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
                        + " WHERE c.relname IN ('zzgd7_hp','zzgd7_hc') AND a.attnum > 0"));

        // A column of a domain is a column depending on that domain, however it came to be one.
        assertEquals("2BP01", stateOf("DROP DOMAIN zzgd7_hd"));
        assertEquals("cannot drop type zzgd7_hd because other objects depend on it",
                messageOf("DROP DOMAIN zzgd7_hd"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP DOMAIN zzgd7_hd"));

        exec("DROP TABLE zzgd7_hc");
        exec("DROP TABLE zzgd7_hp");
        exec("DROP DOMAIN zzgd7_hd");
    }

    @Test
    void noTypeAReaderDefinedTakesATypeModifier() throws Exception {
        exec("CREATE DOMAIN zzgd7_md AS int");
        exec("CREATE TYPE zzgd7_mc AS (x int)");
        exec("CREATE TYPE zzgd7_me AS ENUM ('a')");

        assertEquals("42601", stateOf("CREATE TABLE zzgd7_m1 (a zzgd7_md(5))"));
        assertEquals("type modifier is not allowed for type \"zzgd7_md\"",
                messageOf("CREATE TABLE zzgd7_m1 (a zzgd7_md(5))"));
        assertEquals("type modifier is not allowed for type \"zzgd7_mc\"",
                messageOf("CREATE TABLE zzgd7_m2 (a zzgd7_mc(3))"));
        assertEquals("type modifier is not allowed for type \"zzgd7_me\"",
                messageOf("CREATE TABLE zzgd7_m3 (a zzgd7_me(3))"));

        exec("CREATE TABLE zzgd7_m4 (a int)");
        assertEquals("type modifier is not allowed for type \"zzgd7_md\"",
                messageOf("ALTER TABLE zzgd7_m4 ALTER COLUMN a TYPE zzgd7_md(5)"));
        assertEquals("type modifier is not allowed for type \"zzgd7_mc\"",
                messageOf("ALTER TABLE zzgd7_m4 ADD COLUMN b zzgd7_mc(3)"));

        // A type nothing answers to is reported as that, and the column is settled before it is.
        assertEquals("42704", stateOf("ALTER TABLE zzgd7_m4 ALTER COLUMN a TYPE zzgd7_nosuch"));
        assertEquals("type \"zzgd7_nosuch\" does not exist",
                messageOf("ALTER TABLE zzgd7_m4 ALTER COLUMN a TYPE zzgd7_nosuch"));
        assertEquals("42703", stateOf("ALTER TABLE zzgd7_m4 ALTER COLUMN nosuch TYPE zzgd7_md"));
        assertEquals("column \"nosuch\" of relation \"zzgd7_m4\" does not exist",
                messageOf("ALTER TABLE zzgd7_m4 ALTER COLUMN nosuch TYPE zzgd7_md"));

        exec("DROP TABLE zzgd7_m4");
        exec("DROP TYPE zzgd7_me");
        exec("DROP TYPE zzgd7_mc");
        exec("DROP DOMAIN zzgd7_md");
    }

    // ------------------------------------------------------------ A NOT NULL that is not valid yet

    @Test
    void aNotNullDeclaredNotValidIsRecordedOnTheRelationAndOnEveryChild() throws Exception {
        exec("CREATE TABLE zzgd7_np (i int, j int)");
        exec("CREATE TABLE zzgd7_nc () INHERITS (zzgd7_np)");
        exec("INSERT INTO zzgd7_np VALUES (1, NULL)");
        exec("INSERT INTO zzgd7_nc VALUES (2, NULL)");
        exec("ALTER TABLE zzgd7_np ADD CONSTRAINT zzgd7_nn NOT NULL j NOT VALID");

        assertEquals("zzgd7_nc/zzgd7_nn/n/false/false/1/false/true,"
                        + "zzgd7_np/zzgd7_nn/n/false/true/0/false/true",
                scalar("SELECT string_agg(cl.relname||'/'||c.conname||'/'||c.contype::text||'/'"
                        + "||c.convalidated::text||'/'||c.conislocal::text||'/'"
                        + "||c.coninhcount::text||'/'||c.connoinherit::text||'/'"
                        + "||c.conenforced::text, ',' ORDER BY cl.relname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON cl.oid = c.conrelid"
                        + " WHERE cl.relname IN ('zzgd7_np','zzgd7_nc')"));

        // The clause is part of what the constraint is, so its definition says so too.
        assertEquals("NOT NULL j NOT VALID",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname = 'zzgd7_nn' AND conrelid = 'zzgd7_np'::regclass"));

        // NOT VALID defers the rows already there, never the rule itself: the column is marked
        // NOT NULL on both relations, the two rows stay, and every row written now is held to it.
        assertEquals("zzgd7_nc/true,zzgd7_np/true",
                scalar("SELECT string_agg(cl.relname||'/'||a.attnotnull::text, ','"
                        + " ORDER BY cl.relname) FROM pg_attribute a"
                        + " JOIN pg_class cl ON cl.oid = a.attrelid"
                        + " WHERE cl.relname IN ('zzgd7_np','zzgd7_nc') AND a.attname = 'j'"));
        assertEquals(2, num("SELECT count(*)::int FROM zzgd7_np"));
        assertEquals("23502", stateOf("INSERT INTO zzgd7_np VALUES (3, NULL)"));
        assertEquals("null value in column \"j\" of relation \"zzgd7_np\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzgd7_np VALUES (3, NULL)"));
        assertEquals("null value in column \"j\" of relation \"zzgd7_nc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzgd7_nc VALUES (4, NULL)"));

        exec("DROP TABLE zzgd7_nc");
        exec("DROP TABLE zzgd7_np");
    }

    @Test
    void aRowThatWasAlreadyThereMayStayButMayNotBeWrittenAgain() throws Exception {
        exec("CREATE TABLE zzgd7_u (i int, j int)");
        exec("INSERT INTO zzgd7_u VALUES (1, NULL), (2, 5)");
        exec("ALTER TABLE zzgd7_u ADD CONSTRAINT zzgd7_un NOT NULL j NOT VALID");
        assertEquals("1/null;2/5", rowsOf("SELECT i, j FROM zzgd7_u ORDER BY i"));

        // Writing the row again is writing a null into the column, so it is refused even where
        // the statement never named that column.
        assertEquals("23502", stateOf("UPDATE zzgd7_u SET i = 9 WHERE i = 1"));
        assertEquals("null value in column \"j\" of relation \"zzgd7_u\""
                        + " violates not-null constraint",
                messageOf("UPDATE zzgd7_u SET i = 9 WHERE i = 1"));
        assertEquals("null value in column \"j\" of relation \"zzgd7_u\""
                        + " violates not-null constraint",
                messageOf("UPDATE zzgd7_u SET j = NULL WHERE i = 2"));

        exec("UPDATE zzgd7_u SET j = 3 WHERE i = 1");
        exec("ALTER TABLE zzgd7_u VALIDATE CONSTRAINT zzgd7_un");
        assertEquals("zzgd7_u/true", validatedAcross("zzgd7_un"));

        exec("DROP TABLE zzgd7_u");
    }

    @Test
    void validateConstraintReadsTheRowsAndNamesTheRelationTheyAreReallyIn() throws Exception {
        exec("CREATE TABLE zzgd7_vp (i int, j int)");
        exec("CREATE TABLE zzgd7_vc () INHERITS (zzgd7_vp)");
        exec("INSERT INTO zzgd7_vc VALUES (2, NULL)");
        exec("ALTER TABLE zzgd7_vp ADD NOT NULL j NOT VALID");

        assertEquals("zzgd7_vc/false,zzgd7_vp/false", validatedAcross("zzgd7_vp_j_not_null"));
        assertEquals("23502",
                stateOf("ALTER TABLE zzgd7_vp VALIDATE CONSTRAINT zzgd7_vp_j_not_null"));
        assertEquals("column \"j\" of relation \"zzgd7_vc\" contains null values",
                messageOf("ALTER TABLE zzgd7_vp VALIDATE CONSTRAINT zzgd7_vp_j_not_null"));
        // Asked of the child, the answer names the child too, because that is where the rows are.
        assertEquals("column \"j\" of relation \"zzgd7_vc\" contains null values",
                messageOf("ALTER TABLE zzgd7_vc VALIDATE CONSTRAINT zzgd7_vp_j_not_null"));
        assertEquals("zzgd7_vc/false,zzgd7_vp/false", validatedAcross("zzgd7_vp_j_not_null"));

        // Validating on the child settles the child's copy alone: the relation that declared the
        // rule is still waiting for its own rows to be read.
        exec("UPDATE zzgd7_vc SET j = 7");
        exec("ALTER TABLE zzgd7_vc VALIDATE CONSTRAINT zzgd7_vp_j_not_null");
        assertEquals("zzgd7_vc/true,zzgd7_vp/false", validatedAcross("zzgd7_vp_j_not_null"));
        exec("ALTER TABLE zzgd7_vp VALIDATE CONSTRAINT zzgd7_vp_j_not_null");
        assertEquals("zzgd7_vc/true,zzgd7_vp/true", validatedAcross("zzgd7_vp_j_not_null"));

        // Validating what has already been validated has nothing to do, and the clause is gone
        // from the definition.
        exec("ALTER TABLE zzgd7_vp VALIDATE CONSTRAINT zzgd7_vp_j_not_null");
        assertEquals("NOT NULL j",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname = 'zzgd7_vp_j_not_null'"
                        + " AND conrelid = 'zzgd7_vp'::regclass"));

        exec("DROP TABLE zzgd7_vc");
        exec("DROP TABLE zzgd7_vp");
    }

    @Test
    void aPartitionedTableKeepsItsRowsBelowAndThatIsWhereTheScanLooks() throws Exception {
        exec("CREATE TABLE zzgd7_pp (i int, j int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzgd7_p1 PARTITION OF zzgd7_pp FOR VALUES FROM (1) TO (10)");
        exec("INSERT INTO zzgd7_pp VALUES (2, NULL)");
        exec("ALTER TABLE zzgd7_pp ADD CONSTRAINT zzgd7_pn NOT NULL j NOT VALID");

        assertEquals("zzgd7_p1/false/false/1,zzgd7_pp/false/true/0",
                scalar("SELECT string_agg(cl.relname||'/'||c.convalidated::text||'/'"
                        + "||c.conislocal::text||'/'||c.coninhcount::text, ',' ORDER BY cl.relname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON cl.oid = c.conrelid"
                        + " WHERE c.conname = 'zzgd7_pn'"));
        assertEquals("23502", stateOf("ALTER TABLE zzgd7_pp VALIDATE CONSTRAINT zzgd7_pn"));
        assertEquals("column \"j\" of relation \"zzgd7_p1\" contains null values",
                messageOf("ALTER TABLE zzgd7_pp VALIDATE CONSTRAINT zzgd7_pn"));

        exec("UPDATE zzgd7_pp SET j = 1");
        exec("ALTER TABLE zzgd7_pp VALIDATE CONSTRAINT zzgd7_pn");
        assertEquals("zzgd7_p1/true,zzgd7_pp/true", validatedAcross("zzgd7_pn"));

        exec("DROP TABLE zzgd7_pp");
    }

    @Test
    void aRelationThatJoinsTheHierarchyAfterwardsTakesTheRuleAsValidated() throws Exception {
        // A relation created below the one that declared the rule brings no rows with it, so its
        // own copy holds over everything it stores from the moment it is there.
        exec("CREATE TABLE zzgd7_zp (i int, j int) PARTITION BY RANGE (i)");
        exec("ALTER TABLE zzgd7_zp ADD CONSTRAINT zzgd7_zn NOT NULL j NOT VALID");
        exec("CREATE TABLE zzgd7_z1 PARTITION OF zzgd7_zp FOR VALUES FROM (1) TO (10)");
        assertEquals("zzgd7_z1/true,zzgd7_zp/false", validatedAcross("zzgd7_zn"));
        exec("DROP TABLE zzgd7_zp");

        exec("CREATE TABLE zzgd7_yp (i int, j int)");
        exec("ALTER TABLE zzgd7_yp ADD CONSTRAINT zzgd7_yn NOT NULL j NOT VALID");
        exec("CREATE TABLE zzgd7_yc () INHERITS (zzgd7_yp)");
        assertEquals("zzgd7_yc/true,zzgd7_yp/false", validatedAcross("zzgd7_yn"));

        exec("DROP TABLE zzgd7_yc");
        exec("DROP TABLE zzgd7_yp");
    }

    @Test
    void aValidatedNotNullWillNotBeDeclaredOverOneNobodyHasRead() throws Exception {
        exec("CREATE TABLE zzgd7_b (i int, j int)");
        exec("ALTER TABLE zzgd7_b ADD CONSTRAINT zzgd7_bn NOT NULL j NOT VALID");

        assertEquals("55000",
                stateOf("ALTER TABLE zzgd7_b ADD CONSTRAINT zzgd7_bn2 NOT NULL j"));
        assertEquals("incompatible NOT VALID constraint \"zzgd7_bn\" on relation \"zzgd7_b\"",
                messageOf("ALTER TABLE zzgd7_b ADD CONSTRAINT zzgd7_bn2 NOT NULL j"));
        assertEquals("You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.",
                hintOf("ALTER TABLE zzgd7_b ADD CONSTRAINT zzgd7_bn2 NOT NULL j"));

        // A second NOT VALID declaration is refused for what it would create: nothing. The
        // constraint already there keeps the column, and its own name.
        assertEquals("55000",
                stateOf("ALTER TABLE zzgd7_b ADD CONSTRAINT zzgd7_bn3 NOT NULL j NOT VALID"));
        assertEquals("cannot create not-null constraint \"zzgd7_bn3\" on column \"j\""
                        + " of table \"zzgd7_b\"",
                messageOf("ALTER TABLE zzgd7_b ADD CONSTRAINT zzgd7_bn3 NOT NULL j NOT VALID"));
        assertEquals("zzgd7_bn/false",
                scalar("SELECT string_agg(conname||'/'||convalidated::text, ',' ORDER BY conname)"
                        + " FROM pg_constraint WHERE conrelid = 'zzgd7_b'::regclass"));

        exec("DROP TABLE zzgd7_b");
    }

    @Test
    void aPrimaryKeyWillNotBeBuiltOverANotNullNobodyHasRead() throws Exception {
        exec("CREATE TABLE zzgd7_kk (i int, j int)");
        exec("INSERT INTO zzgd7_kk VALUES (1, NULL)");
        exec("ALTER TABLE zzgd7_kk ADD CONSTRAINT zzgd7_kn NOT NULL j NOT VALID");

        assertEquals("55000", stateOf("ALTER TABLE zzgd7_kk ADD PRIMARY KEY (j)"));
        assertEquals("cannot create primary key on column \"j\"",
                messageOf("ALTER TABLE zzgd7_kk ADD PRIMARY KEY (j)"));
        assertEquals("The constraint \"zzgd7_kn\" on column \"j\" of table \"zzgd7_kk\","
                        + " marked NOT VALID, is incompatible with a primary key.",
                detailOf("ALTER TABLE zzgd7_kk ADD PRIMARY KEY (j)"));
        assertEquals("You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.",
                hintOf("ALTER TABLE zzgd7_kk ADD PRIMARY KEY (j)"));
        assertEquals("zzgd7_kn/n/false",
                scalar("SELECT string_agg(conname||'/'||contype::text||'/'||convalidated::text,"
                        + " ',' ORDER BY conname) FROM pg_constraint"
                        + " WHERE conrelid = 'zzgd7_kk'::regclass"));

        // SET NOT NULL reads the rows, so it settles what the declaration left open.
        assertEquals("23502", stateOf("ALTER TABLE zzgd7_kk ALTER COLUMN j SET NOT NULL"));
        assertEquals("column \"j\" of relation \"zzgd7_kk\" contains null values",
                messageOf("ALTER TABLE zzgd7_kk ALTER COLUMN j SET NOT NULL"));
        assertEquals("zzgd7_kk/false", validatedAcross("zzgd7_kn"));

        exec("UPDATE zzgd7_kk SET j = 4");
        exec("ALTER TABLE zzgd7_kk ALTER COLUMN j SET NOT NULL");
        assertEquals("zzgd7_kk/true", validatedAcross("zzgd7_kn"));

        exec("DROP TABLE zzgd7_kk");
    }

    @Test
    void notValidDoesNotReopenARuleTheColumnHasAlreadyBeenHeldTo() throws Exception {
        exec("CREATE TABLE zzgd7_hh (i int, j int NOT NULL)");
        assertEquals("55000",
                stateOf("ALTER TABLE zzgd7_hh ADD CONSTRAINT zzgd7_hn NOT NULL j NOT VALID"));
        assertEquals("cannot create not-null constraint \"zzgd7_hn\" on column \"j\""
                        + " of table \"zzgd7_hh\"",
                messageOf("ALTER TABLE zzgd7_hh ADD CONSTRAINT zzgd7_hn NOT NULL j NOT VALID"));

        // The declaration creates nothing, and nothing is what it leaves behind: the constraint
        // already there keeps its name and stays as validated as it was.
        assertEquals("zzgd7_hh_j_not_null/true",
                scalar("SELECT string_agg(conname||'/'||convalidated::text, ',' ORDER BY conname)"
                        + " FROM pg_constraint WHERE conrelid = 'zzgd7_hh'::regclass"));

        exec("DROP TABLE zzgd7_hh");
    }

    @Test
    void droppingANotValidNotNullTakesTheRuleWithIt() throws Exception {
        exec("CREATE TABLE zzgd7_dd (i int, j int)");
        exec("INSERT INTO zzgd7_dd VALUES (1, NULL)");
        exec("ALTER TABLE zzgd7_dd ADD CONSTRAINT zzgd7_dn NOT NULL j NOT VALID");
        exec("ALTER TABLE zzgd7_dd DROP CONSTRAINT zzgd7_dn");
        assertEquals("i/false/true/0,j/false/true/0", attsOf("zzgd7_dd"));

        // And so does DROP NOT NULL written on the column.
        exec("ALTER TABLE zzgd7_dd ADD CONSTRAINT zzgd7_dn NOT NULL j NOT VALID");
        exec("ALTER TABLE zzgd7_dd ALTER COLUMN j DROP NOT NULL");
        assertNull(notNullsOf("zzgd7_dd"));
        exec("INSERT INTO zzgd7_dd VALUES (2, NULL)");
        assertEquals(2, num("SELECT count(*)::int FROM zzgd7_dd"));

        exec("DROP TABLE zzgd7_dd");
    }

    @Test
    void aValidationThatIsRolledBackLeavesTheRowsUnread() throws Exception {
        exec("CREATE TABLE zzgd7_rr (i int, j int)");
        exec("INSERT INTO zzgd7_rr VALUES (1, NULL)");
        exec("ALTER TABLE zzgd7_rr ADD CONSTRAINT zzgd7_rn NOT NULL j NOT VALID");
        exec("UPDATE zzgd7_rr SET j = 2");

        rolledBack(() -> {
            exec("ALTER TABLE zzgd7_rr VALIDATE CONSTRAINT zzgd7_rn");
            assertEquals("zzgd7_rr/true", validatedAcross("zzgd7_rn"));
        });
        assertEquals("zzgd7_rr/false", validatedAcross("zzgd7_rn"));

        // And the declaration itself is undone with the transaction that made it.
        rolledBack(() -> exec("ALTER TABLE zzgd7_rr ADD CONSTRAINT zzgd7_rn2 NOT NULL i NOT VALID"));
        assertEquals("i/false/true/0,j/true/true/0", attsOf("zzgd7_rr"));

        exec("DROP TABLE zzgd7_rr");
    }

    @Test
    void aCheckDeclaredNotValidIsValidatedOverTheRowsTheRelationStandsFor() throws Exception {
        exec("CREATE TABLE zzgd7_cp (i int, j int)");
        exec("CREATE TABLE zzgd7_cc () INHERITS (zzgd7_cp)");
        exec("INSERT INTO zzgd7_cc VALUES (1, 0)");
        exec("ALTER TABLE zzgd7_cp ADD CONSTRAINT zzgd7_ck CHECK (j > 0) NOT VALID");

        assertEquals("CHECK ((j > 0)) NOT VALID",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname = 'zzgd7_ck' AND conrelid = 'zzgd7_cp'::regclass"));
        // The row already there may stay; a row written now is held to the rule.
        assertEquals("new row for relation \"zzgd7_cc\" violates check constraint \"zzgd7_ck\"",
                messageOf("INSERT INTO zzgd7_cc VALUES (2, -1)"));
        assertEquals("23514", stateOf("ALTER TABLE zzgd7_cp VALIDATE CONSTRAINT zzgd7_ck"));
        assertEquals("check constraint \"zzgd7_ck\" of relation \"zzgd7_cc\""
                        + " is violated by some row",
                messageOf("ALTER TABLE zzgd7_cp VALIDATE CONSTRAINT zzgd7_ck"));

        exec("UPDATE zzgd7_cc SET j = 5");
        exec("ALTER TABLE zzgd7_cp VALIDATE CONSTRAINT zzgd7_ck");
        assertEquals("zzgd7_cc/true,zzgd7_cp/true", validatedAcross("zzgd7_ck"));
        assertEquals("CHECK ((j > 0))",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname = 'zzgd7_ck' AND conrelid = 'zzgd7_cp'::regclass"));

        exec("DROP TABLE zzgd7_cc");
        exec("DROP TABLE zzgd7_cp");
    }

    @Test
    void validatingACheckOnAChildSettlesThatChildAlone() throws Exception {
        exec("CREATE TABLE zzgd7_c3 (i int, j int)");
        exec("CREATE TABLE zzgd7_c4 () INHERITS (zzgd7_c3)");
        exec("INSERT INTO zzgd7_c4 VALUES (1, 5)");
        exec("ALTER TABLE zzgd7_c3 ADD CONSTRAINT zzgd7_ck2 CHECK (j > 0) NOT VALID");
        exec("ALTER TABLE zzgd7_c4 VALIDATE CONSTRAINT zzgd7_ck2");

        assertEquals("zzgd7_c3/false,zzgd7_c4/true", validatedAcross("zzgd7_ck2"));

        exec("DROP TABLE zzgd7_c4");
        exec("DROP TABLE zzgd7_c3");
    }
    // ------------------------------------------------------------ What a failed statement leaves behind in the relations its triggers wrote to

    /** Creates a log relation, a target and a routine that notes each row and raises on i = 2. */
    private static void noisyTrigger(String name, String when) throws SQLException {
        exec("CREATE TABLE " + name + "_log (n int)");
        exec("CREATE TABLE " + name + "_t (i int)");
        exec("CREATE FUNCTION " + name + "_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO " + name + "_log VALUES (NEW.i);"
                + " IF NEW.i = 2 THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW; END $$");
        exec("CREATE TRIGGER " + name + "_tr " + when + " INSERT ON " + name + "_t"
                + " FOR EACH ROW EXECUTE FUNCTION " + name + "_note()");
    }

    private static void dropNoisyTrigger(String name) throws SQLException {
        exec("DROP TABLE " + name + "_t CASCADE");
        exec("DROP TABLE " + name + "_log CASCADE");
        exec("DROP FUNCTION " + name + "_note() CASCADE");
    }

    @Test
    void whatAnAfterTriggerWroteElsewhereGoesWithTheStatementThatFailed() throws Exception {
        noisyTrigger("zzjt_af", "AFTER");

        // The first row was written and noted before the second row raised.
        assertEquals("boom", messageOf("INSERT INTO zzjt_af_t VALUES (1),(2),(3)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_af_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_af_log"));

        // The control: a statement that succeeds keeps every write its trigger made.
        exec("INSERT INTO zzjt_af_t VALUES (7),(8)");
        assertEquals("7,8", column("SELECT n FROM zzjt_af_log ORDER BY n"));
        assertEquals(2, num("SELECT count(*)::int FROM zzjt_af_t"));

        // And a later failure takes back only its own statement's writes.
        assertEquals("P0001", stateOf("INSERT INTO zzjt_af_t VALUES (5),(2)"));
        assertEquals("7,8", column("SELECT n FROM zzjt_af_log ORDER BY n"));
        assertEquals(2, num("SELECT count(*)::int FROM zzjt_af_t"));

        dropNoisyTrigger("zzjt_af");
    }

    @Test
    void whatABeforeTriggerWroteElsewhereGoesWithTheStatementThatFailed() throws Exception {
        noisyTrigger("zzjt_bf", "BEFORE");

        assertEquals("boom", messageOf("INSERT INTO zzjt_bf_t VALUES (1),(2),(3)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_bf_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_bf_log"));

        exec("INSERT INTO zzjt_bf_t VALUES (7),(8)");
        assertEquals("7,8", column("SELECT n FROM zzjt_bf_log ORDER BY n"));
        assertEquals(2, num("SELECT count(*)::int FROM zzjt_bf_t"));

        assertEquals("P0001", stateOf("INSERT INTO zzjt_bf_t VALUES (5),(2)"));
        assertEquals("7,8", column("SELECT n FROM zzjt_bf_log ORDER BY n"));
        assertEquals(2, num("SELECT count(*)::int FROM zzjt_bf_t"));

        dropNoisyTrigger("zzjt_bf");
    }

    @Test
    void insideATransactionBlockTheFailedStatementsTriggerWritesGoTooAndNoOthers() throws Exception {
        noisyTrigger("zzjt_rb", "AFTER");
        exec("INSERT INTO zzjt_rb_t VALUES (9)");

        conn.setAutoCommit(false);
        try {
            assertEquals("P0001", stateOf("INSERT INTO zzjt_rb_t VALUES (1),(2),(3)"));
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
        // Nothing of the failed statement is left, and the row written before the block stands.
        assertEquals(1, num("SELECT count(*)::int FROM zzjt_rb_t"));
        assertEquals("9", column("SELECT n FROM zzjt_rb_log ORDER BY n"));

        // The control: a transaction that commits keeps what its triggers wrote.
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO zzjt_rb_t VALUES (7)");
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals("7,9", column("SELECT i FROM zzjt_rb_t ORDER BY i"));
        assertEquals("7,9", column("SELECT n FROM zzjt_rb_log ORDER BY n"));

        dropNoisyTrigger("zzjt_rb");
    }

    @Test
    void rollingBackToASavepointLeavesTheTriggerWritesMadeBeforeIt() throws Exception {
        noisyTrigger("zzjt_sp", "AFTER");

        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO zzjt_sp_t VALUES (5)");
            Savepoint sp = conn.setSavepoint("zzjt_sp_point");
            assertEquals("P0001", stateOf("INSERT INTO zzjt_sp_t VALUES (1),(2),(3)"));
            conn.rollback(sp);

            // The write from the statement before the savepoint is still there.
            assertEquals(1, num("SELECT count(*)::int FROM zzjt_sp_t"));
            assertEquals(1, num("SELECT count(*)::int FROM zzjt_sp_log"));

            // And the transaction goes on from there.
            exec("INSERT INTO zzjt_sp_t VALUES (6)");
            assertEquals(2, num("SELECT count(*)::int FROM zzjt_sp_log"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals("5,6", column("SELECT n FROM zzjt_sp_log ORDER BY n"));
        assertEquals("5,6", column("SELECT i FROM zzjt_sp_t ORDER BY i"));

        dropNoisyTrigger("zzjt_sp");
    }

    @Test
    void anExceptionHandlerUndoesTheTriggerWritesOfTheStatementItCaught() throws Exception {
        noisyTrigger("zzjt_eh", "AFTER");
        String caught = "DO $$ BEGIN BEGIN INSERT INTO zzjt_eh_t VALUES (1),(2),(3);"
                + " EXCEPTION WHEN others THEN NULL; END; END $$";

        // Outside a transaction block, with the handler swallowing the error.
        exec(caught);
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_eh_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_eh_log"));

        // Inside one, where the transaction goes on afterwards and commits.
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO zzjt_eh_t VALUES (9)");
            exec(caught);
            assertEquals(1, num("SELECT count(*)::int FROM zzjt_eh_t"));
            assertEquals(1, num("SELECT count(*)::int FROM zzjt_eh_log"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals("9", column("SELECT n FROM zzjt_eh_log ORDER BY n"));

        dropNoisyTrigger("zzjt_eh");
    }

    @Test
    void whatAFailedUpdateDeleteAndMergeLeftElsewhereGoesTheSameWay() throws Exception {
        exec("CREATE TABLE zzjt_ud_log (n int)");
        exec("CREATE TABLE zzjt_ud_t (i int, v int)");
        exec("INSERT INTO zzjt_ud_t VALUES (1,10),(2,20),(3,30)");
        exec("CREATE FUNCTION zzjt_ud_u() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzjt_ud_log VALUES (NEW.i);"
                + " IF NEW.i = 2 THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_ud_d() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzjt_ud_log VALUES (OLD.i);"
                + " IF OLD.i = 2 THEN RAISE EXCEPTION 'boom'; END IF; RETURN OLD; END $$");

        exec("CREATE TRIGGER zzjt_ud_au AFTER UPDATE ON zzjt_ud_t"
                + " FOR EACH ROW EXECUTE FUNCTION zzjt_ud_u()");
        assertEquals("P0001", stateOf("UPDATE zzjt_ud_t SET v = v + 1"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ud_log"));
        assertEquals("10,20,30", column("SELECT v FROM zzjt_ud_t ORDER BY i"));

        // The control for UPDATE: the rows it does reach are noted and kept.
        exec("UPDATE zzjt_ud_t SET v = v + 1 WHERE i <> 2");
        assertEquals("1,3", column("SELECT n FROM zzjt_ud_log ORDER BY n"));
        assertEquals("11,20,31", column("SELECT v FROM zzjt_ud_t ORDER BY i"));
        exec("DELETE FROM zzjt_ud_log");

        exec("DROP TRIGGER zzjt_ud_au ON zzjt_ud_t");
        exec("CREATE TRIGGER zzjt_ud_am AFTER UPDATE ON zzjt_ud_t"
                + " FOR EACH ROW EXECUTE FUNCTION zzjt_ud_u()");
        assertEquals("P0001", stateOf("MERGE INTO zzjt_ud_t t"
                + " USING (SELECT 1 AS i UNION SELECT 2) s ON t.i = s.i"
                + " WHEN MATCHED THEN UPDATE SET v = t.v + 100"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ud_log"));
        assertEquals("11,20,31", column("SELECT v FROM zzjt_ud_t ORDER BY i"));

        // The control for MERGE.
        exec("MERGE INTO zzjt_ud_t t USING (SELECT 1 AS i UNION SELECT 3) s ON t.i = s.i"
                + " WHEN MATCHED THEN UPDATE SET v = t.v + 100");
        assertEquals("1,3", column("SELECT n FROM zzjt_ud_log ORDER BY n"));
        assertEquals("111,20,131", column("SELECT v FROM zzjt_ud_t ORDER BY i"));
        exec("DELETE FROM zzjt_ud_log");
        exec("DROP TRIGGER zzjt_ud_am ON zzjt_ud_t");

        exec("CREATE TRIGGER zzjt_ud_ad AFTER DELETE ON zzjt_ud_t"
                + " FOR EACH ROW EXECUTE FUNCTION zzjt_ud_d()");
        assertEquals("P0001", stateOf("DELETE FROM zzjt_ud_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ud_log"));
        assertEquals(3, num("SELECT count(*)::int FROM zzjt_ud_t"));

        // The control for DELETE.
        exec("DELETE FROM zzjt_ud_t WHERE i <> 2");
        assertEquals("1,3", column("SELECT n FROM zzjt_ud_log ORDER BY n"));
        assertEquals(1, num("SELECT count(*)::int FROM zzjt_ud_t"));

        exec("DROP TABLE zzjt_ud_t CASCADE");
        exec("DROP TABLE zzjt_ud_log CASCADE");
        exec("DROP FUNCTION zzjt_ud_u() CASCADE");
        exec("DROP FUNCTION zzjt_ud_d() CASCADE");
    }

    @Test
    void whatATriggerWroteThroughARuleGoesWithTheStatementThatFailed() throws Exception {
        exec("CREATE TABLE zzjt_ru_log (n int)");
        exec("CREATE TABLE zzjt_ru_via (n int)");
        exec("CREATE RULE zzjt_ru_r AS ON INSERT TO zzjt_ru_via"
                + " DO ALSO INSERT INTO zzjt_ru_log VALUES (NEW.n)");
        exec("CREATE TABLE zzjt_ru_t (i int)");
        exec("CREATE FUNCTION zzjt_ru_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzjt_ru_via VALUES (NEW.i);"
                + " IF NEW.i = 2 THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW; END $$");
        exec("CREATE TRIGGER zzjt_ru_tr BEFORE INSERT ON zzjt_ru_t"
                + " FOR EACH ROW EXECUTE FUNCTION zzjt_ru_note()");

        // Two relations away from the one the statement named, and it still goes.
        assertEquals("P0001", stateOf("INSERT INTO zzjt_ru_t VALUES (1),(2),(3)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ru_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ru_via"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ru_log"));

        exec("INSERT INTO zzjt_ru_t VALUES (4),(5)");
        assertEquals("4,5", column("SELECT n FROM zzjt_ru_via ORDER BY n"));
        assertEquals("4,5", column("SELECT n FROM zzjt_ru_log ORDER BY n"));

        exec("DROP TABLE zzjt_ru_t CASCADE");
        exec("DROP TABLE zzjt_ru_via CASCADE");
        exec("DROP TABLE zzjt_ru_log CASCADE");
        exec("DROP FUNCTION zzjt_ru_note() CASCADE");
    }

    @Test
    void whatATriggerWroteThroughAFunctionItCalledGoesTheSameWay() throws Exception {
        exec("CREATE TABLE zzjt_fn_log (n int)");
        exec("CREATE FUNCTION zzjt_fn_w(v int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzjt_fn_log VALUES (v); RETURN v; END $$");
        exec("CREATE FUNCTION zzjt_fn_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " PERFORM zzjt_fn_w(NEW.i);"
                + " IF NEW.i = 2 THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW; END $$");
        exec("CREATE TABLE zzjt_fn_t (i int)");
        exec("CREATE TRIGGER zzjt_fn_tr AFTER INSERT ON zzjt_fn_t"
                + " FOR EACH ROW EXECUTE FUNCTION zzjt_fn_note()");

        assertEquals("P0001", stateOf("INSERT INTO zzjt_fn_t VALUES (1),(2),(3)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_fn_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_fn_log"));

        exec("INSERT INTO zzjt_fn_t VALUES (4)");
        assertEquals("4", column("SELECT n FROM zzjt_fn_log ORDER BY n"));

        exec("DROP TABLE zzjt_fn_t CASCADE");
        exec("DROP TABLE zzjt_fn_log CASCADE");
        exec("DROP FUNCTION zzjt_fn_note() CASCADE");
        exec("DROP FUNCTION zzjt_fn_w(int) CASCADE");
    }

    @Test
    void whatAStatementLevelTriggerWroteGoesWithTheStatementThatFailed() throws Exception {
        exec("CREATE TABLE zzjt_st_log (n int)");
        exec("CREATE TABLE zzjt_st_t (i int)");
        exec("CREATE FUNCTION zzjt_st_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzjt_st_log VALUES (0); RETURN NULL; END $$");
        exec("CREATE FUNCTION zzjt_st_raise() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " RAISE EXCEPTION 'boom'; END $$");
        exec("CREATE TRIGGER zzjt_st_b BEFORE INSERT ON zzjt_st_t"
                + " FOR EACH STATEMENT EXECUTE FUNCTION zzjt_st_note()");
        exec("CREATE TRIGGER zzjt_st_a AFTER INSERT ON zzjt_st_t"
                + " FOR EACH STATEMENT EXECUTE FUNCTION zzjt_st_raise()");

        assertEquals("P0001", stateOf("INSERT INTO zzjt_st_t VALUES (1)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_st_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_st_log"));

        exec("DROP TRIGGER zzjt_st_a ON zzjt_st_t");
        exec("INSERT INTO zzjt_st_t VALUES (1)");
        assertEquals(1, num("SELECT count(*)::int FROM zzjt_st_t"));
        assertEquals(1, num("SELECT count(*)::int FROM zzjt_st_log"));

        exec("DROP TABLE zzjt_st_t CASCADE");
        exec("DROP TABLE zzjt_st_log CASCADE");
        exec("DROP FUNCTION zzjt_st_note() CASCADE");
        exec("DROP FUNCTION zzjt_st_raise() CASCADE");
    }

    @Test
    void aTriggersWriteGoesWhenItIsAConstraintThatRefusesALaterRow() throws Exception {
        exec("CREATE TABLE zzjt_ck_log (n int)");
        exec("CREATE TABLE zzjt_ck_t (i int CHECK (i < 3))");
        exec("CREATE FUNCTION zzjt_ck_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzjt_ck_log VALUES (NEW.i); RETURN NEW; END $$");
        exec("CREATE TRIGGER zzjt_ck_tr BEFORE INSERT ON zzjt_ck_t"
                + " FOR EACH ROW EXECUTE FUNCTION zzjt_ck_note()");

        // The trigger raises nothing; it is the constraint on the third row that refuses,
        // and the notes the trigger made for the first two go with it.
        assertEquals("new row for relation \"zzjt_ck_t\" violates check constraint"
                        + " \"zzjt_ck_t_i_check\"",
                messageOf("INSERT INTO zzjt_ck_t VALUES (1),(2),(9)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ck_t"));
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_ck_log"));

        exec("INSERT INTO zzjt_ck_t VALUES (1),(2)");
        assertEquals("1,2", column("SELECT n FROM zzjt_ck_log ORDER BY n"));

        exec("DROP TABLE zzjt_ck_t CASCADE");
        exec("DROP TABLE zzjt_ck_log CASCADE");
        exec("DROP FUNCTION zzjt_ck_note() CASCADE");
    }

    // ------------------------------------------------------------ A copy of a partitioned table's trigger may not carry the row out of the partition it was routed to

    @Test
    void aCopiedTriggerThatAssignsAColumnItsOwnValueHasRewrittenTheRow() throws Exception {
        exec("CREATE FUNCTION zzjt_mv_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := NEW.k + 10; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_mv_same() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.i := NEW.i; RETURN NEW; END $$");
        exec("CREATE TABLE zzjt_mv_q (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE zzjt_mv_q0 PARTITION OF zzjt_mv_q FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzjt_mv_q1 PARTITION OF zzjt_mv_q FOR VALUES FROM (10) TO (20)");
        // The trigger on the partition carries the key out of it; the copy of the partitioned
        // table's trigger then runs and assigns a column its own value.
        exec("CREATE TRIGGER zzjt_mv_a_move BEFORE INSERT ON zzjt_mv_q0 FOR EACH ROW"
                + " EXECUTE FUNCTION zzjt_mv_bump()");
        exec("CREATE TRIGGER zzjt_mv_b_same BEFORE INSERT ON zzjt_mv_q FOR EACH ROW"
                + " EXECUTE FUNCTION zzjt_mv_same()");

        org.postgresql.util.ServerErrorMessage e =
                fieldsOf("INSERT INTO zzjt_mv_q VALUES (1, 5)");
        assertEquals("0A000", e.getSQLState());
        assertEquals("moving row to another partition during a BEFORE FOR EACH ROW trigger"
                + " is not supported", e.getMessage());
        assertEquals("Before executing trigger \"zzjt_mv_b_same\", the row was to be in partition"
                + " \"public.zzjt_mv_q0\".", e.getDetail());
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_mv_q"));

        // A row that stays where it was routed is written as usual.
        exec("INSERT INTO zzjt_mv_q VALUES (2, 12)");
        assertEquals("2/12", rowsOf("SELECT i, k FROM zzjt_mv_q1"));

        exec("DROP TABLE zzjt_mv_q CASCADE");
        exec("DROP FUNCTION zzjt_mv_same() CASCADE");
        exec("DROP FUNCTION zzjt_mv_bump() CASCADE");
    }

    @Test
    void whichWritesToNewCountAsARewriteOfTheRowAndWhichDoNot() throws Exception {
        // PostgreSQL decides whether the copy rewrote the row by whether the routine handed back
        // a tuple other than the one it was given, and PL/pgSQL builds a new one the moment
        // anything is assigned into NEW. So a self-assignment is a rewrite (0A000) while a bare
        // RETURN NEW is not, and the writer is told about the partition constraint instead.
        exec("CREATE FUNCTION zzjt_wr_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := NEW.k + 10; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_wr_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_wr_into() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " SELECT NEW.i INTO NEW.i; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_wr_sub() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.a[1] := NEW.a[1]; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_wr_rec() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " SELECT NEW.i, NEW.k, NEW.a INTO NEW; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_wr_self() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW := NEW; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_wr_read() RETURNS trigger LANGUAGE plpgsql AS $$"
                + " DECLARE v int; BEGIN v := NEW.i; RETURN NEW; END $$");
        exec("CREATE TABLE zzjt_wr_q (i int, k int, a int[]) PARTITION BY RANGE (k)");
        exec("CREATE TABLE zzjt_wr_q0 PARTITION OF zzjt_wr_q FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzjt_wr_q1 PARTITION OF zzjt_wr_q FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER zzjt_wr_a_move BEFORE INSERT ON zzjt_wr_q0 FOR EACH ROW"
                + " EXECUTE FUNCTION zzjt_wr_bump()");

        // Anything assigned into NEW, however little it changes, is a rewrite.
        assertEquals("0A000", copyVerdict("zzjt_wr_into"));
        assertEquals("0A000", copyVerdict("zzjt_wr_sub"));
        assertEquals("0A000", copyVerdict("zzjt_wr_rec"));

        // Reading NEW is not, and neither is handing PostgreSQL back the record it gave.
        assertEquals("23514", copyVerdict("zzjt_wr_noop"));
        assertEquals("23514", copyVerdict("zzjt_wr_self"));
        assertEquals("23514", copyVerdict("zzjt_wr_read"));

        exec("DROP TABLE zzjt_wr_q CASCADE");
        for (String routine : new String[] {"zzjt_wr_bump", "zzjt_wr_noop", "zzjt_wr_into",
                "zzjt_wr_sub", "zzjt_wr_rec", "zzjt_wr_self", "zzjt_wr_read"}) {
            exec("DROP FUNCTION " + routine + "() CASCADE");
        }
    }

    /** Fires one routine as a copy of the partitioned table's trigger and reads the refusal back. */
    private static String copyVerdict(String routine) throws SQLException {
        exec("CREATE TRIGGER zzjt_wr_b_x BEFORE INSERT ON zzjt_wr_q FOR EACH ROW"
                + " EXECUTE FUNCTION " + routine + "()");
        String state = stateOf("INSERT INTO zzjt_wr_q VALUES (1, 5, '{1,2}')");
        exec("DROP TRIGGER zzjt_wr_b_x ON zzjt_wr_q");
        return state;
    }

    @Test
    void aCopyThatRanBeforeTheRowWasMovedIsNotTheOneBlamed() throws Exception {
        // The check runs the moment each trigger hands the row back, so a copy that ran while the
        // row still fitted is past before the trigger declared on the partition moves it -- and
        // what the writer is told is the plain partition-constraint refusal.
        exec("CREATE FUNCTION zzjt_or_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := NEW.k + 10; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzjt_or_selfk() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := NEW.k; RETURN NEW; END $$");
        exec("CREATE TABLE zzjt_or_q (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE zzjt_or_q0 PARTITION OF zzjt_or_q FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzjt_or_q1 PARTITION OF zzjt_or_q FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER zzjt_or_a_selfk BEFORE INSERT ON zzjt_or_q FOR EACH ROW"
                + " EXECUTE FUNCTION zzjt_or_selfk()");
        exec("CREATE TRIGGER zzjt_or_b_move BEFORE INSERT ON zzjt_or_q0 FOR EACH ROW"
                + " EXECUTE FUNCTION zzjt_or_bump()");

        org.postgresql.util.ServerErrorMessage e =
                fieldsOf("INSERT INTO zzjt_or_q VALUES (4, 5)");
        assertEquals("23514", e.getSQLState());
        assertEquals("new row for relation \"zzjt_or_q0\" violates partition constraint",
                e.getMessage());
        assertEquals("Failing row contains (4, 15).", e.getDetail());
        assertEquals(0, num("SELECT count(*)::int FROM zzjt_or_q"));

        exec("DROP TABLE zzjt_or_q CASCADE");
        exec("DROP FUNCTION zzjt_or_selfk() CASCADE");
        exec("DROP FUNCTION zzjt_or_bump() CASCADE");
    }

    // ------------------------------------------------------------ What may stand in a partition key

    @Test
    void aGeneratedColumnMayNotStandInAPartitionKeyWhateverTheStrategy() throws Exception {
        String stored = "(i int, k int GENERATED ALWAYS AS (i * 2) STORED)";
        for (String by : new String[] {"RANGE (k)", "LIST (k)", "HASH (k)"}) {
            String sql = "CREATE TABLE zzjt_pk " + stored + " PARTITION BY " + by;
            assertEquals("42P17", stateOf(sql), by);
            assertEquals("cannot use generated column in partition key", messageOf(sql), by);
            assertEquals("Column \"k\" is a generated column.", detailOf(sql), by);
        }

        // A virtual generated column is refused for the same reason as a stored one.
        assertEquals("42P17", stateOf("CREATE TABLE zzjt_pk (i int,"
                + " k int GENERATED ALWAYS AS (i * 2) VIRTUAL) PARTITION BY RANGE (k)"));

        // In company, and inside an expression, and with the column named in the DETAIL.
        assertEquals("42P17", stateOf("CREATE TABLE zzjt_pk " + stored
                + " PARTITION BY RANGE (i, k)"));
        assertEquals("42P17", stateOf("CREATE TABLE zzjt_pk " + stored
                + " PARTITION BY RANGE ((k + 1))"));
        assertEquals("Column \"k\" is a generated column.",
                detailOf("CREATE TABLE zzjt_pk (i int, k int GENERATED ALWAYS AS (i*2) STORED,"
                        + " m int) PARTITION BY RANGE ((k + m))"));
        // Where two of them stand in one expression, the leftmost column is named.
        assertEquals("Column \"i\" is a generated column.",
                detailOf("CREATE TABLE zzjt_pk (i int GENERATED ALWAYS AS (1) STORED,"
                        + " k int GENERATED ALWAYS AS (2) STORED) PARTITION BY RANGE ((k + i))"));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzjt_pk'"));
    }

    @Test
    void whichFaultAKeyIsRefusedForFollowsTheOrderPostgresReadsItIn() throws Exception {
        String gen = "(i int, k int GENERATED ALWAYS AS (i * 2) STORED)";

        // The key elements are walked left to right, so whichever stands first is reported.
        assertEquals("cannot use generated column in partition key",
                messageOf("CREATE TABLE zzjt_pk " + gen + " PARTITION BY RANGE (k, xmin)"));
        assertEquals("cannot use system column \"xmin\" in partition key",
                messageOf("CREATE TABLE zzjt_pk " + gen + " PARTITION BY RANGE (xmin, k)"));

        // A name, an aggregate and a function that is not IMMUTABLE are all settled while the key
        // list is turned into expressions, which is before any element is looked at as a column.
        assertEquals("42703", stateOf("CREATE TABLE zzjt_pk " + gen
                + " PARTITION BY RANGE ((k + nosuch))"));
        assertEquals("42803", stateOf("CREATE TABLE zzjt_pk (i int,"
                + " k int GENERATED ALWAYS AS (i*2) STORED) PARTITION BY RANGE (k, (sum(i)))"));
        assertEquals("42P17", stateOf("CREATE TABLE zzjt_pk (i int,"
                + " k int GENERATED ALWAYS AS (i*2) STORED)"
                + " PARTITION BY RANGE ((random()::int), k)"));
        assertEquals("functions in partition key expression must be marked IMMUTABLE",
                messageOf("CREATE TABLE zzjt_pk (i int, k int GENERATED ALWAYS AS (i*2) STORED)"
                        + " PARTITION BY RANGE ((random()::int), k)"));

        // A bare name that does not exist is reported as a partition key's own; the same name
        // inside an expression is reported the way any expression's is.
        assertEquals("column \"nosuch\" named in partition key does not exist",
                messageOf("CREATE TABLE zzjt_pk (i int, k int) PARTITION BY RANGE (nosuch)"));
        assertEquals("column \"nosuch\" does not exist",
                messageOf("CREATE TABLE zzjt_pk (i int, k int) PARTITION BY RANGE ((i + nosuch))"));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzjt_pk'"));
    }

    @Test
    void aGeneratedColumnOutsideTheKeyIsComputedInThePartitionTheRowWasRoutedTo() throws Exception {
        exec("CREATE TABLE zzjt_gp (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)"
                + " PARTITION BY LIST (s)");
        exec("CREATE TABLE zzjt_gp_a PARTITION OF zzjt_gp FOR VALUES IN ('a')");
        exec("INSERT INTO zzjt_gp (i, s) VALUES (3, 'a')");

        assertEquals("3/6/a", rowsOf("SELECT i, k, s FROM zzjt_gp ORDER BY i"));
        assertEquals("3/6", rowsOf("SELECT i, k FROM zzjt_gp_a ORDER BY i"));
        assertEquals("LIST (s)", scalar("SELECT pg_get_partkeydef('zzjt_gp'::regclass)"));
        // The partition holds the column as a generated one of its own.
        assertEquals("s", scalar("SELECT attgenerated FROM pg_attribute"
                + " WHERE attrelid = 'zzjt_gp_a'::regclass AND attname = 'k'"));

        exec("DROP TABLE zzjt_gp CASCADE");

        // An identity column and a serial are not generated columns, and may stand in a key.
        exec("CREATE TABLE zzjt_ip (i int GENERATED ALWAYS AS IDENTITY, k int)"
                + " PARTITION BY RANGE (i)");
        assertEquals("RANGE (i)", scalar("SELECT pg_get_partkeydef('zzjt_ip'::regclass)"));
        exec("DROP TABLE zzjt_ip CASCADE");

        exec("CREATE TABLE zzjt_sq (i serial, k int) PARTITION BY RANGE (i)");
        assertEquals("RANGE (i)", scalar("SELECT pg_get_partkeydef('zzjt_sq'::regclass)"));
        exec("DROP TABLE zzjt_sq CASCADE");
    }

    // ------------------------------------------------------------ What a stored expression may name

    @Test
    void aCallOverArgumentTypesTheNameIsNotDeclaredOverIsRefusedWhereItIsWritten() throws Exception {
        // Each of these names a routine PostgreSQL does declare, and none of them is declared over
        // the argument types the definition writes, so the definition is what is refused.
        String[][] refused = {
                {"CREATE TABLE zzsd_c (a int, CHECK (lower(a) > 'x'))",
                        "function lower(integer) does not exist"},
                {"CREATE TABLE zzsd_c (a int, CHECK (upper(a) > 'x'))",
                        "function upper(integer) does not exist"},
                {"CREATE TABLE zzsd_c (a int, CHECK (length(a) > 0))",
                        "function length(integer) does not exist"},
                {"CREATE TABLE zzsd_c (a int, CHECK (substr(a, 1) > 'x'))",
                        "function substr(integer, integer) does not exist"},
                {"CREATE TABLE zzsd_c (a int, CHECK (repeat(a, 2) > 'x'))",
                        "function repeat(integer, integer) does not exist"},
                {"CREATE TABLE zzsd_c (a int, CHECK (btrim(a) > 'x'))",
                        "function btrim(integer) does not exist"},
                // The type the argument has is what the name is resolved with, however it is written.
                {"CREATE TABLE zzsd_c (a int, CHECK (lower(a + 1) > 'x'))",
                        "function lower(integer) does not exist"},
                {"CREATE TABLE zzsd_c (b boolean, CHECK (lower(b) > 'x'))",
                        "function lower(boolean) does not exist"},
        };
        for (String[] one : refused) {
            org.postgresql.util.ServerErrorMessage e = fieldsOf(one[0]);
            assertEquals("42883", e.getSQLState(), one[0]);
            assertEquals(one[1], e.getMessage(), one[0]);
            assertEquals("No function matches the given name and argument types."
                    + " You might need to add explicit type casts.", e.getHint(), one[0]);
        }
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_c'"));
    }

    @Test
    void everyCallTheNameIsDeclaredOverIsStoredWithTheDefinition() throws Exception {
        exec("CREATE DOMAIN zzsd_dom AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE zzsd_ok1 (k text, CHECK (lower(k) <> 'zz'))");
        exec("CREATE TABLE zzsd_ok2 (a int, CHECK (abs(a) > 0))");
        exec("CREATE TABLE zzsd_ok3 (a int, CHECK (lower(a::text) > 'x'))");
        exec("CREATE TABLE zzsd_ok4 (a int, CHECK (lower('x') > 'x'))");
        exec("CREATE TABLE zzsd_ok5 (a int, CHECK (to_char(a, 'FM999') > 'x'))");
        exec("CREATE TABLE zzsd_ok6 (a int, k text, CHECK (coalesce(k, 'x') <> ''))");
        exec("CREATE TABLE zzsd_ok7 (a int, CHECK (greatest(a, 1) > 0))");
        // A type name written as a call of one argument is a coercion, and is stored as one.
        exec("CREATE TABLE zzsd_ok8 (a int, CHECK (text(a) > 'x'))");
        exec("CREATE TABLE zzsd_ok9 (a int, CHECK (int8(a) > 0))");
        exec("CREATE TABLE zzsd_ok10 (a int, CHECK (zzsd_dom(a) IS NOT NULL))");

        assertEquals(10, num("SELECT count(*)::int FROM information_schema.tables"
                + " WHERE table_name LIKE 'zzsd\\_ok%'"));

        for (int i = 1; i <= 10; i++) exec("DROP TABLE zzsd_ok" + i);
        exec("DROP DOMAIN zzsd_dom");
    }

    @Test
    void aQualifierIsResolvedAsASchemaBeforeAnythingOfThatNameIsLookedFor() throws Exception {
        exec("CREATE TABLE zzsd_base (a int, k text)");

        // Wherever the call is written, the schema is what the writer is told about.
        String[] wherever = {
                "CREATE TABLE zzsd_q (a int, CHECK (zzsd_nos.nosuchfunc(a) > 0))",
                "CREATE TABLE zzsd_q (a int, b int GENERATED ALWAYS AS (zzsd_nos.f(a)) STORED)",
                "CREATE TABLE zzsd_q (a int DEFAULT zzsd_nos.f(1))",
                "CREATE INDEX zzsd_qi ON zzsd_base (a) WHERE zzsd_nos.f(a) > 0",
                "ALTER TABLE zzsd_base ALTER COLUMN k TYPE text USING zzsd_nos.f(a)",
        };
        for (String sql : wherever) {
            org.postgresql.util.ServerErrorMessage e = fieldsOf(sql);
            assertEquals("3F000", e.getSQLState(), sql);
            assertEquals("schema \"zzsd_nos\" does not exist", e.getMessage(), sql);
        }

        // The arguments are transformed first, so a column that is not there comes first.
        assertEquals("42703",
                stateOf("CREATE TABLE zzsd_q (a int, CHECK (zzsd_nos.f(nosuchcol) > 0))"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("CREATE TABLE zzsd_q (a int, CHECK (zzsd_nos.f(nosuchcol) > 0))"));

        // An unquoted qualifier is folded the way every other unquoted name is.
        assertEquals("schema \"nosuchzzm\" does not exist",
                messageOf("CREATE TABLE zzsd_q (a int, CHECK (NoSuchZzm.NoFunc(a) > 0))"));

        // A qualifier that does name a schema holds the name to what is really in it.
        exec("CREATE TABLE zzsd_q1 (a int, CHECK (pg_catalog.abs(a) > 0))");
        assertEquals("42883",
                stateOf("CREATE TABLE zzsd_q2 (a int, CHECK (pg_catalog.lower(a) > 'x'))"));
        assertEquals("function pg_catalog.lower(integer) does not exist",
                messageOf("CREATE TABLE zzsd_q2 (a int, CHECK (pg_catalog.lower(a) > 'x'))"));

        // A quoted qualifier keeps the case the writer wrote.
        exec("CREATE SCHEMA \"zzsd_MixEd\"");
        exec("CREATE FUNCTION \"zzsd_MixEd\".f(int) RETURNS int AS 'SELECT $1' LANGUAGE sql");
        exec("CREATE TABLE zzsd_q3 (a int, CHECK (\"zzsd_MixEd\".f(a) > 0))");

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_q'"));
        exec("DROP TABLE zzsd_q3");
        exec("DROP FUNCTION \"zzsd_MixEd\".f(int)");
        exec("DROP SCHEMA \"zzsd_MixEd\"");
        exec("DROP TABLE zzsd_q1");
        exec("DROP TABLE zzsd_base");
    }

    @Test
    void aNameStandsWhereItIsWrittenInsideACaseAnInListABetweenBoundOrAConstructor()
            throws Exception {
        String[] everywhere = {
                "CHECK (CASE WHEN nosuchcol > 0 THEN true END)",
                "CHECK (CASE a WHEN nosuchcol THEN true ELSE false END)",
                "CHECK (CASE WHEN a > 0 THEN nosuchcol > 1 END)",
                "CHECK (CASE WHEN a > 0 THEN true ELSE nosuchcol > 1 END)",
                "CHECK (a IN (nosuchcol))",
                "CHECK (nosuchcol IN (1, 2))",
                "CHECK (a BETWEEN nosuchcol AND 3)",
                "CHECK (a BETWEEN 1 AND nosuchcol)",
                "CHECK (a = ANY (ARRAY[nosuchcol]))",
                "CHECK ((ROW(nosuchcol, 1)) IS NOT NULL)",
                "CHECK ((nosuchcol) IS NOT NULL)",
                // ...beside the places the reader already reached
                "CHECK (coalesce(nosuchcol, 1) > 0)",
                "CHECK (nosuchcol::int > 0)",
                "CHECK (a > nosuchcol)",
                "CHECK (abs(abs(nosuchcol)) > 0)",
                "CHECK (nosuchcol LIKE 'x')",
                "CHECK (nullif(nosuchcol, 1) > 0)",
                "CHECK (a > (nosuchcol + 1))",
                "CHECK (a IS NOT NULL AND nosuchcol > 0)",
        };
        for (String check : everywhere) {
            String sql = "CREATE TABLE zzsd_n (a int, " + check + ")";
            assertEquals("42703", stateOf(sql), sql);
            assertEquals("column \"nosuchcol\" does not exist", messageOf(sql), sql);
        }
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_n'"));
    }

    @Test
    void aCompositeTypeWrittenAsACallOfOneArgumentIsNotACastPostgresPerforms() throws Exception {
        // A composite has no input function to read another type's value with, so the one-argument
        // form is a call of a name no routine answers to.
        exec("CREATE TYPE zzsd_comp AS (x int, y int)");

        org.postgresql.util.ServerErrorMessage e =
                fieldsOf("CREATE TABLE zzsd_n (a int, CHECK (zzsd_comp(a) IS NOT NULL))");
        assertEquals("42883", e.getSQLState());
        assertEquals("function zzsd_comp(integer) does not exist", e.getMessage());
        assertEquals("No function matches the given name and argument types."
                + " You might need to add explicit type casts.", e.getHint());

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_n'"));
        exec("DROP TYPE zzsd_comp");
    }

    @Test
    void everyPlaceAStoredExpressionIsWrittenSettlesItsCallsThere() throws Exception {
        exec("CREATE TABLE zzsd_p (a int, k text)");

        String[] places = {
                "CREATE TABLE zzsd_pg (a int, b text GENERATED ALWAYS AS (lower(a)) STORED)",
                "CREATE TABLE zzsd_pd (a int DEFAULT lower(1))",
                "CREATE INDEX zzsd_pi ON zzsd_p (a) WHERE lower(a) > 'x'",
                "CREATE INDEX zzsd_pi ON zzsd_p (lower(a))",
                "ALTER TABLE zzsd_p ALTER COLUMN k TYPE text USING lower(a)",
                "ALTER TABLE zzsd_p ADD COLUMN c int DEFAULT lower(1)",
                "ALTER TABLE zzsd_p ALTER COLUMN a SET DEFAULT lower(1)",
        };
        for (String sql : places) {
            assertEquals("42883", stateOf(sql), sql);
            assertEquals("function lower(integer) does not exist", messageOf(sql), sql);
        }

        // And a name written inside a CASE arm reaches every one of them too.
        String[] arms = {
                "CREATE TABLE zzsd_pg (a int, b int GENERATED ALWAYS AS"
                        + " (CASE WHEN nosuchcol > 0 THEN 1 END) STORED)",
                "CREATE INDEX zzsd_pi ON zzsd_p (a) WHERE CASE WHEN nosuchcol > 0 THEN true END",
                "CREATE INDEX zzsd_pi ON zzsd_p ((CASE WHEN nosuchcol > 0 THEN 1 END))",
                "ALTER TABLE zzsd_p ALTER COLUMN k TYPE text USING"
                        + " (CASE WHEN nosuchcol > 0 THEN 'x' END)",
                // An IN list written as an index key is an expression, not a call of its own.
                "CREATE INDEX zzsd_pi ON zzsd_p ((a IN (nosuchcol)))",
        };
        for (String sql : arms) {
            assertEquals("42703", stateOf(sql), sql);
            assertEquals("column \"nosuchcol\" does not exist", messageOf(sql), sql);
        }

        assertEquals("a/integer,k/text", scalar("SELECT string_agg(attname||'/'"
                + "||format_type(atttypid, atttypmod), ',' ORDER BY attnum) FROM pg_attribute"
                + " WHERE attrelid = 'zzsd_p'::regclass AND attnum > 0"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname IN ('zzsd_pg', 'zzsd_pd', 'zzsd_pi')"));
        exec("DROP TABLE zzsd_p");
    }

    @Test
    void anIndexPredicateIsTransformedBeforeItsKeys() throws Exception {
        exec("CREATE TABLE zzsd_ix (a int, k text)");

        // Both halves are wrong, and the predicate is the half PostgreSQL reads first.
        assertEquals("column \"nosuchpred\" does not exist",
                messageOf("CREATE UNIQUE INDEX zzsd_ixi ON zzsd_ix (lower(nosuchkey))"
                        + " WHERE nosuchpred IS NULL"));
        assertEquals("column \"nosuchpred\" does not exist",
                messageOf("CREATE INDEX zzsd_ixi ON zzsd_ix (k) WHERE nosuchpred IS NULL"));
        assertEquals("column \"nosuchkey\" does not exist",
                messageOf("CREATE INDEX zzsd_ixi ON zzsd_ix (lower(nosuchkey)) WHERE a > 0"));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_ixi'"));
        exec("DROP TABLE zzsd_ix");
    }

    @Test
    void aGeneratedColumnMayNameTheRelationBeingDefinedWithItsSchema() throws Exception {
        exec("CREATE TABLE zzsd_g1 (a int, b int GENERATED ALWAYS AS (public.zzsd_g1.a + 1) STORED)");
        exec("INSERT INTO zzsd_g1 VALUES (7)");
        assertEquals("7/8", rowsOf("SELECT a, b FROM zzsd_g1"));

        exec("CREATE TABLE zzsd_g2 (a int, CHECK (public.zzsd_g2.a > 0))");
        exec("INSERT INTO zzsd_g2 VALUES (3)");
        assertEquals("3", rowsOf("SELECT a FROM zzsd_g2"));

        // A relation of that name in another schema is not this one.
        org.postgresql.util.ServerErrorMessage e = fieldsOf("CREATE TABLE zzsd_g3 (a int,"
                + " b int GENERATED ALWAYS AS (zzsd_nos.zzsd_g3.a + 1) STORED)");
        assertEquals("42P01", e.getSQLState());
        assertEquals("invalid reference to FROM-clause entry for table \"zzsd_g3\"", e.getMessage());
        assertEquals("There is an entry for table \"zzsd_g3\", but it cannot be referenced from"
                + " this part of the query.", e.getDetail());

        // A relation created in another schema names that schema, and public is not that schema.
        exec("CREATE SCHEMA zzsd_s");
        exec("CREATE TABLE zzsd_s.zzsd_g4 (a int,"
                + " b int GENERATED ALWAYS AS (zzsd_s.zzsd_g4.a + 1) STORED)");
        exec("INSERT INTO zzsd_s.zzsd_g4 VALUES (5)");
        assertEquals("5/6", rowsOf("SELECT a, b FROM zzsd_s.zzsd_g4"));
        assertEquals("42P01", stateOf("CREATE TABLE zzsd_s.zzsd_g5 (a int,"
                + " b int GENERATED ALWAYS AS (public.zzsd_g5.a + 1) STORED)"));

        // The column under the full name is still looked for.
        assertEquals("column zzsd_g6.nosuchcol does not exist",
                messageOf("CREATE TABLE zzsd_g6 (a int,"
                        + " b int GENERATED ALWAYS AS (public.zzsd_g6.nosuchcol + 1) STORED)"));

        exec("DROP TABLE zzsd_s.zzsd_g4");
        exec("DROP SCHEMA zzsd_s");
        exec("DROP TABLE zzsd_g1");
        exec("DROP TABLE zzsd_g2");
    }

    @Test
    void theCallsThatDoResolveAreStoredAndComputedInEveryOneOfThosePlaces() throws Exception {
        exec("CREATE TABLE zzsd_r (a int, k text)");
        exec("INSERT INTO zzsd_r VALUES (3, 'Ab')");
        exec("CREATE INDEX zzsd_ri1 ON zzsd_r (a) WHERE lower(k) > 'x'");
        exec("CREATE INDEX zzsd_ri2 ON zzsd_r (lower(k))");
        exec("CREATE INDEX zzsd_ri3 ON zzsd_r (abs(a))");
        exec("CREATE INDEX zzsd_ri4 ON zzsd_r ((CASE WHEN a > 0 THEN k ELSE 'z' END))");
        exec("CREATE INDEX zzsd_ri5 ON zzsd_r (a) WHERE a IN (1, 2, 3)");
        exec("CREATE INDEX zzsd_ri6 ON zzsd_r (a) WHERE a BETWEEN 1 AND 9");
        exec("CREATE INDEX zzsd_ri7 ON zzsd_r ((a = ANY (ARRAY[1, 2])))");
        assertEquals(7, num("SELECT count(*)::int FROM pg_class WHERE relname LIKE 'zzsd\\_ri%'"));

        exec("ALTER TABLE zzsd_r ALTER COLUMN k TYPE text USING lower(k)");
        assertEquals("3/ab", rowsOf("SELECT a, k FROM zzsd_r"));

        exec("ALTER TABLE zzsd_r ADD COLUMN c text DEFAULT lower('Q')");
        exec("ALTER TABLE zzsd_r ALTER COLUMN a SET DEFAULT abs(-4)");
        exec("INSERT INTO zzsd_r (k) VALUES ('z')");
        assertEquals("3/ab/q;4/z/q", rowsOf("SELECT a, k, c FROM zzsd_r ORDER BY k"));
        exec("DROP TABLE zzsd_r");

        exec("CREATE TABLE zzsd_rg (a int, k text,"
                + " b text GENERATED ALWAYS AS (lower(k)) STORED,"
                + " c int GENERATED ALWAYS AS (abs(a) + 1) STORED,"
                + " d text GENERATED ALWAYS AS (CASE WHEN a > 0 THEN 'p' ELSE 'n' END) STORED,"
                + " e text GENERATED ALWAYS AS (lower(a::text)) STORED)");
        exec("INSERT INTO zzsd_rg (a, k) VALUES (-2, 'Ab')");
        assertEquals("-2/Ab/ab/3/n/-2", rowsOf("SELECT a, k, b, c, d, e FROM zzsd_rg"));
        exec("DROP TABLE zzsd_rg");

        exec("CREATE TABLE zzsd_rd (a text DEFAULT lower('Q'), b int DEFAULT abs(-3),"
                + " c text DEFAULT to_char(5, 'FM999'), d int DEFAULT greatest(1, 2),"
                + " e text DEFAULT coalesce(NULL, 'x'))");
        exec("INSERT INTO zzsd_rd DEFAULT VALUES");
        assertEquals("q/3/5/2/x", rowsOf("SELECT a, b, c, d, e FROM zzsd_rd"));
        exec("DROP TABLE zzsd_rd");
    }

    // ------------------------------------------------------------ The type a column reads back as

    @Test
    void anArrayColumnRefusesADefaultOfAnythingButAnArrayItCanTake() throws Exception {
        String[][] refused = {
                {"CREATE TABLE zzsd_a (a int[] DEFAULT 1)",
                        "column \"a\" is of type integer[] but default expression is of type integer"},
                {"CREATE TABLE zzsd_a (a int[] DEFAULT true)",
                        "column \"a\" is of type integer[] but default expression is of type boolean"},
                {"CREATE TABLE zzsd_a (a int[] DEFAULT now())",
                        "column \"a\" is of type integer[] but default expression is of type"
                                + " timestamp with time zone"},
                {"CREATE TABLE zzsd_a (a int[] DEFAULT '{a}'::text[])",
                        "column \"a\" is of type integer[] but default expression is of type text[]"},
                {"CREATE TABLE zzsd_a (a text[] DEFAULT 1)",
                        "column \"a\" is of type text[] but default expression is of type integer"},
                {"CREATE TABLE zzsd_a (a text[] DEFAULT 'x'::text)",
                        "column \"a\" is of type text[] but default expression is of type text"},
                // The array type is named without the modifier the column was declared with.
                {"CREATE TABLE zzsd_a (a varchar(5)[] DEFAULT 1)",
                        "column \"a\" is of type character varying[] but default expression is of"
                                + " type integer"},
                {"CREATE TABLE zzsd_a (a bool[] DEFAULT 1)",
                        "column \"a\" is of type boolean[] but default expression is of type integer"},
                {"CREATE TABLE zzsd_a (a date[] DEFAULT 1)",
                        "column \"a\" is of type date[] but default expression is of type integer"},
                {"CREATE TABLE zzsd_a (a numeric[] DEFAULT 1)",
                        "column \"a\" is of type numeric[] but default expression is of type integer"},
                // A generation expression is judged by the same rule and in the same words.
                {"CREATE TABLE zzsd_a (a int[], b int[] GENERATED ALWAYS AS (1) STORED)",
                        "column \"b\" is of type integer[] but default expression is of type integer"},
        };
        for (String[] one : refused) {
            org.postgresql.util.ServerErrorMessage e = fieldsOf(one[0]);
            assertEquals("42804", e.getSQLState(), one[0]);
            assertEquals(one[1], e.getMessage(), one[0]);
            assertEquals("You will need to rewrite or cast the expression.", e.getHint(), one[0]);
        }
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_a'"));

        // ...and a column added later, and a default set later, go the same way.
        exec("CREATE TABLE zzsd_aa (a int)");
        assertEquals("42804", stateOf("ALTER TABLE zzsd_aa ADD COLUMN b int[] DEFAULT 1"));
        assertEquals("column \"b\" is of type integer[] but default expression is of type integer",
                messageOf("ALTER TABLE zzsd_aa ADD COLUMN b int[] DEFAULT 1"));
        exec("ALTER TABLE zzsd_aa ADD COLUMN b int[] DEFAULT '{1,2}'");
        assertEquals("42804", stateOf("ALTER TABLE zzsd_aa ALTER COLUMN b SET DEFAULT 5"));
        exec("DROP TABLE zzsd_aa");
    }

    @Test
    void anArrayColumnTakesEveryDefaultWhoseElementsReachItsElementType() throws Exception {
        exec("CREATE TABLE zzsd_ok (a int[] DEFAULT '{1,2}', b int[] DEFAULT '{1,2}'::int[],"
                + " c int[] DEFAULT ARRAY[1,2], d text[] DEFAULT ARRAY[1,2],"
                + " e text[] DEFAULT '{1,2}'::int[], f int8[] DEFAULT '{1}'::int4[],"
                + " g int[] DEFAULT NULL, h int[] DEFAULT '{}')");
        exec("INSERT INTO zzsd_ok DEFAULT VALUES");
        assertEquals("{1,2}/{1,2}/{1,2}/{1,2}/{1,2}/{1}/null/{}",
                rowsOf("SELECT a, b, c, d, e, f, g, h FROM zzsd_ok"));
        exec("DROP TABLE zzsd_ok");

        exec("CREATE TABLE zzsd_al (a int)");
        exec("ALTER TABLE zzsd_al ADD COLUMN b int[] DEFAULT '{1,2}'");
        exec("ALTER TABLE zzsd_al ADD COLUMN c text[] DEFAULT ARRAY['x']");
        exec("ALTER TABLE zzsd_al ADD COLUMN d int8[] DEFAULT '{7}'::int4[]");
        exec("ALTER TABLE zzsd_al ALTER COLUMN b SET DEFAULT ARRAY[5, 6]");
        exec("INSERT INTO zzsd_al (a) VALUES (1)");
        assertEquals("1/{5,6}/{x}/{7}", rowsOf("SELECT a, b, c, d FROM zzsd_al"));
        exec("DROP TABLE zzsd_al");

        exec("CREATE TABLE zzsd_al2 (a int[], b int[] GENERATED ALWAYS AS (a) STORED)");
        exec("INSERT INTO zzsd_al2 (a) VALUES ('{4,5}')");
        assertEquals("{4,5}/{4,5}", rowsOf("SELECT a, b FROM zzsd_al2"));
        exec("DROP TABLE zzsd_al2");
    }

    @Test
    void aBooleanReadIntoAStringColumnIsSpelledOutInFull() throws Exception {
        exec("CREATE TABLE zzsd_b (a varchar, b text, c char(5), d varchar(4), e bool)");
        exec("INSERT INTO zzsd_b (a,b,c,d,e) VALUES (true, true, true, true, true)");

        assertEquals("true/true/true/t", rowsOf("SELECT a, b, d, e FROM zzsd_b"));
        // The blank-padded column holds 'true ' and is measured without its padding.
        assertEquals("4/4/4/4",
                rowsOf("SELECT length(a), length(b), length(c), length(d) FROM zzsd_b"));

        exec("UPDATE zzsd_b SET a = false, b = false");
        assertEquals("false/false", rowsOf("SELECT a, b FROM zzsd_b"));
        exec("DROP TABLE zzsd_b");

        // A DEFAULT of a boolean on a string column reads back the same way.
        exec("CREATE TABLE zzsd_bd (a varchar DEFAULT true, b text DEFAULT false,"
                + " c varchar(4) DEFAULT true)");
        exec("INSERT INTO zzsd_bd DEFAULT VALUES");
        assertEquals("true/false/true", rowsOf("SELECT a, b, c FROM zzsd_bd"));
        exec("DROP TABLE zzsd_bd");

        // And so does a retype out of boolean into one.
        exec("CREATE TABLE zzsd_br (f bool)");
        exec("INSERT INTO zzsd_br VALUES (true)");
        exec("ALTER TABLE zzsd_br ALTER COLUMN f TYPE varchar");
        assertEquals("true", rowsOf("SELECT f FROM zzsd_br"));
        exec("DROP TABLE zzsd_br");

        // The letter is still what a boolean is written as inside a container, and an explicit
        // cast still spells it out.
        assertEquals("{t,f}/true/false",
                rowsOf("SELECT ARRAY[true,false], true::text, false::text"));
    }

    @Test
    void aColumnDeclaredAsARangeIsAColumnOfThatRange() throws Exception {
        exec("CREATE TYPE zzsd_rng AS RANGE (subtype = int4)");
        exec("CREATE TABLE zzsd_rt (a zzsd_rng)");
        exec("INSERT INTO zzsd_rt VALUES ('[1,3)')");
        assertEquals("[1,3)/1/3", rowsOf("SELECT a, lower(a), upper(a) FROM zzsd_rt"));

        // The catalogue names the range, and reports the layout the range has.
        assertEquals("a/zzsd_rng/-1/i/x", rowsOf("SELECT attname,"
                + " format_type(atttypid, atttypmod), attlen, attalign, attstorage"
                + " FROM pg_attribute WHERE attrelid = 'zzsd_rt'::regclass AND attnum > 0"
                + " ORDER BY attnum"));
        assertEquals("zzsd_rng/USER-DEFINED", rowsOf("SELECT udt_name, data_type"
                + " FROM information_schema.columns WHERE table_name = 'zzsd_rt'"));

        // A column of the range is a column depending on it.
        org.postgresql.util.ServerErrorMessage e = fieldsOf("DROP TYPE zzsd_rng");
        assertEquals("2BP01", e.getSQLState());
        assertEquals("cannot drop type zzsd_rng because other objects depend on it", e.getMessage());
        assertEquals("column a of table zzsd_rt depends on type zzsd_rng", e.getDetail());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", e.getHint());

        assertEquals("zzsd_rng", scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = 'zzsd_rt'::regclass AND attname = 'a'"));
        exec("DROP TABLE zzsd_rt");
        exec("DROP TYPE zzsd_rng");
    }

    @Test
    void aTextColumnDoesNotReachAReadersOwnTypeWithoutAUsingClause() throws Exception {
        exec("CREATE TYPE zzsd_rng AS RANGE (subtype = int4)");
        exec("CREATE TYPE zzsd_enum AS ENUM ('a', 'b')");
        exec("CREATE DOMAIN zzsd_dm AS int");
        exec("CREATE TABLE zzsd_rc (a text, b text, c text)");

        String[][] refused = {
                {"ALTER TABLE zzsd_rc ALTER COLUMN a TYPE zzsd_rng",
                        "column \"a\" cannot be cast automatically to type zzsd_rng",
                        "You might need to specify \"USING a::zzsd_rng\"."},
                {"ALTER TABLE zzsd_rc ALTER COLUMN b TYPE zzsd_enum",
                        "column \"b\" cannot be cast automatically to type zzsd_enum",
                        "You might need to specify \"USING b::zzsd_enum\"."},
                {"ALTER TABLE zzsd_rc ALTER COLUMN c TYPE zzsd_dm",
                        "column \"c\" cannot be cast automatically to type zzsd_dm",
                        "You might need to specify \"USING c::zzsd_dm\"."},
        };
        for (String[] one : refused) {
            org.postgresql.util.ServerErrorMessage e = fieldsOf(one[0]);
            assertEquals("42804", e.getSQLState(), one[0]);
            assertEquals(one[1], e.getMessage(), one[0]);
            assertEquals(one[2], e.getHint(), one[0]);
        }
        assertEquals("a/text,b/text,c/text", scalar("SELECT string_agg(attname||'/'"
                + "||format_type(atttypid, atttypmod), ',' ORDER BY attnum) FROM pg_attribute"
                + " WHERE attrelid = 'zzsd_rc'::regclass AND attnum > 0"));

        // Told how, it goes.
        exec("ALTER TABLE zzsd_rc ALTER COLUMN a TYPE zzsd_rng USING a::zzsd_rng");
        assertEquals("zzsd_rng", scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = 'zzsd_rc'::regclass AND attname = 'a'"));

        exec("DROP TABLE zzsd_rc");
        exec("DROP DOMAIN zzsd_dm");
        exec("DROP TYPE zzsd_enum");
        exec("DROP TYPE zzsd_rng");
    }

    @Test
    void anArrayOfADomainAnEnumOrARangeReadsBackWithItsBrackets() throws Exception {
        exec("CREATE TYPE zzsd_rng AS RANGE (subtype = int4)");
        exec("CREATE TYPE zzsd_enum AS ENUM ('a', 'b', 'c')");
        exec("CREATE DOMAIN zzsd_dm AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE zzsd_ua (a zzsd_dm[], b zzsd_enum[], c zzsd_rng[], d int[])");

        assertEquals("a/zzsd_dm[],b/zzsd_enum[],c/zzsd_rng[],d/integer[]",
                scalar("SELECT string_agg(attname||'/'||format_type(atttypid, atttypmod), ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzsd_ua'::regclass AND attnum > 0"));

        // And the column holds a list of that type's values.
        exec("INSERT INTO zzsd_ua (a, b) VALUES ('{1,2}', '{a,b}')");
        assertEquals("{1,2}/{a,b}/1/b/2/2", rowsOf("SELECT a, b, a[1], b[2],"
                + " array_length(a,1), array_length(b,1) FROM zzsd_ua"));
        exec("DROP TABLE zzsd_ua");

        // A default, a constructor and a cast all reach one.
        exec("CREATE TABLE zzsd_ar (a zzsd_dm[] DEFAULT '{1,2}', b zzsd_enum[] DEFAULT '{a,b}',"
                + " c int[] DEFAULT ARRAY[9])");
        exec("INSERT INTO zzsd_ar DEFAULT VALUES");
        assertEquals("{1,2}/{a,b}/{9}", rowsOf("SELECT a, b, c FROM zzsd_ar"));
        assertEquals("2/2/a",
                rowsOf("SELECT array_length(a, 1), a[2], b[1] FROM zzsd_ar"));
        assertEquals(1, num("SELECT count(*)::int FROM zzsd_ar WHERE 1 = ANY (a)"));
        assertEquals("a;b", rowsOf("SELECT unnest(b) FROM zzsd_ar ORDER BY 1"));
        exec("INSERT INTO zzsd_ar (a, b, c) VALUES (ARRAY[3, 4]::zzsd_dm[],"
                + " ARRAY['c']::zzsd_enum[], ARRAY[1])");
        assertEquals("{3,4}/{c}/{1};{1,2}/{a,b}/{9}",
                rowsOf("SELECT a, b, c FROM zzsd_ar ORDER BY c"));

        exec("DROP TABLE zzsd_ar");
        exec("DROP DOMAIN zzsd_dm");
        exec("DROP TYPE zzsd_enum");
        exec("DROP TYPE zzsd_rng");
    }

    @Test
    void aDomainsOwnRulesAreEveryElementsRules() throws Exception {
        exec("CREATE DOMAIN zzsd_pos AS int CHECK (VALUE > 0)");
        exec("CREATE DOMAIN zzsd_nn AS int NOT NULL");
        exec("CREATE TABLE zzsd_da (a zzsd_pos[], b zzsd_nn[])");

        assertEquals("23514", stateOf("INSERT INTO zzsd_da (a) VALUES ('{-1}')"));
        assertEquals("value for domain zzsd_pos violates check constraint \"zzsd_pos_check\"",
                messageOf("INSERT INTO zzsd_da (a) VALUES ('{-1}')"));
        // Every element, however many dimensions deep.
        assertEquals("value for domain zzsd_pos violates check constraint \"zzsd_pos_check\"",
                messageOf("INSERT INTO zzsd_da (a) VALUES ('{{1,2},{3,-4}}')"));
        assertEquals("23502", stateOf("INSERT INTO zzsd_da (b) VALUES ('{NULL}')"));
        assertEquals("domain zzsd_nn does not allow null values",
                messageOf("INSERT INTO zzsd_da (b) VALUES ('{NULL}')"));

        // A null array holds no value of the domain, so there is nothing to judge; a CHECK that
        // answers NULL does not violate; and an empty array holds nothing either.
        exec("INSERT INTO zzsd_da (a, b) VALUES ('{1,2}', NULL)");
        exec("INSERT INTO zzsd_da (a) VALUES ('{NULL}')");
        exec("INSERT INTO zzsd_da (a) VALUES ('{}')");
        assertEquals(3, num("SELECT count(*)::int FROM zzsd_da"));
        assertEquals("{};{1,2};{NULL}", rowsOf("SELECT a FROM zzsd_da ORDER BY 1"));

        exec("DROP TABLE zzsd_da");
        exec("DROP DOMAIN zzsd_pos");
        exec("DROP DOMAIN zzsd_nn");
    }

    @Test
    void aCheckOverADomainTypedColumnReadsTheColumnDownToTheBaseType() throws Exception {
        exec("CREATE DOMAIN zzsd_di AS int");
        exec("CREATE DOMAIN zzsd_dt AS text");
        exec("CREATE DOMAIN zzsd_dn AS numeric(8,2)");
        exec("CREATE DOMAIN zzsd_dv AS varchar(5)");
        exec("CREATE TABLE zzsd_dc (a zzsd_di, b zzsd_dt, c zzsd_dn, d zzsd_dv, e int, f text)");

        String[][] written = {
                {"zzsd_x1", "CHECK (a > 1)", "CHECK (((a)::integer > 1))"},
                {"zzsd_x2", "CHECK (b <> 'q')", "CHECK (((b)::text <> 'q'::text))"},
                {"zzsd_x3", "CHECK (c > 1)", "CHECK (((c)::numeric > (1)::numeric))"},
                {"zzsd_x4", "CHECK (d <> 'q')", "CHECK (((d)::text <> 'q'::text))"},
                // No operator, so nothing to read down to.
                {"zzsd_x5", "CHECK (a IS NOT NULL)", "CHECK ((a IS NOT NULL))"},
                {"zzsd_x6", "CHECK (a = e)", "CHECK (((a)::integer = e))"},
                {"zzsd_x7", "CHECK (b = f)", "CHECK (((b)::text = f))"},
                {"zzsd_x8", "CHECK (a + 1 > 2)", "CHECK ((((a)::integer + 1) > 2))"},
                {"zzsd_x9", "CHECK (length(b) > 1)", "CHECK ((length((b)::text) > 1))"},
                {"zzsd_xa", "CHECK (a BETWEEN 1 AND 9)",
                        "CHECK ((((a)::integer >= 1) AND ((a)::integer <= 9)))"},
                {"zzsd_xb", "CHECK (b LIKE 'a%')", "CHECK (((b)::text ~~ 'a%'::text))"},
                {"zzsd_xc", "CHECK (a IN (1, 2))", "CHECK (((a)::integer = ANY (ARRAY[1, 2])))"},
                {"zzsd_xd", "CHECK (-a < 0)", "CHECK (((- (a)::integer) < 0))"},
        };
        for (String[] one : written) {
            exec("ALTER TABLE zzsd_dc ADD CONSTRAINT " + one[0] + " " + one[1]);
        }
        for (String[] one : written) {
            assertEquals(one[2], scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                    + " WHERE conname = '" + one[0] + "'"), one[1]);
        }
        // The information schema carries the same text without the CHECK ( ) around it.
        assertEquals("((b)::text <> 'q'::text)", scalar("SELECT check_clause"
                + " FROM information_schema.check_constraints"
                + " WHERE constraint_name = 'zzsd_x2'"));

        exec("DROP TABLE zzsd_dc");
        exec("DROP DOMAIN zzsd_di");
        exec("DROP DOMAIN zzsd_dt");
        exec("DROP DOMAIN zzsd_dn");
        exec("DROP DOMAIN zzsd_dv");
    }

    @Test
    void aRetypeToADomainRewritesTheChecksThatReadTheColumn() throws Exception {
        exec("CREATE DOMAIN zzsd_pos AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE zzsd_m (a int, b int, c int, d int, e int, f int, g int, h int)");
        exec("ALTER TABLE zzsd_m ADD CONSTRAINT zzsd_k1 CHECK (a > 1)");
        exec("ALTER TABLE zzsd_m ADD CONSTRAINT zzsd_k2 CHECK (b > 1)");
        exec("ALTER TABLE zzsd_m ADD CONSTRAINT zzsd_k3 CHECK (c > 1)");
        exec("ALTER TABLE zzsd_m ADD CONSTRAINT zzsd_k4 CHECK (d IS NOT NULL)");
        exec("ALTER TABLE zzsd_m ADD CONSTRAINT zzsd_k5 CHECK (e + 1 > 2)");
        exec("ALTER TABLE zzsd_m ADD CONSTRAINT zzsd_k6 CHECK (f > 1 AND g > 2)");
        exec("ALTER TABLE zzsd_m ADD CONSTRAINT zzsd_k7 CHECK (h > 1)");

        exec("ALTER TABLE zzsd_m ALTER COLUMN a TYPE bigint");
        exec("ALTER TABLE zzsd_m ALTER COLUMN b TYPE numeric");
        exec("ALTER TABLE zzsd_m ALTER COLUMN c TYPE zzsd_pos");
        exec("ALTER TABLE zzsd_m ALTER COLUMN d TYPE zzsd_pos");
        exec("ALTER TABLE zzsd_m ALTER COLUMN e TYPE zzsd_pos");
        exec("ALTER TABLE zzsd_m ALTER COLUMN f TYPE zzsd_pos");
        exec("ALTER TABLE zzsd_m ALTER COLUMN h TYPE smallint");

        // Only the columns that became a domain carry a reading down to the base type; a widening
        // between built-ins leaves the operator where it was, and the one whose operator gained a
        // numeric operand puts the conversion on the constant.
        assertEquals("zzsd_k1=CHECK ((a > 1));"
                        + "zzsd_k2=CHECK ((b > (1)::numeric));"
                        + "zzsd_k3=CHECK (((c)::integer > 1));"
                        + "zzsd_k4=CHECK ((d IS NOT NULL));"
                        + "zzsd_k5=CHECK ((((e)::integer + 1) > 2));"
                        + "zzsd_k6=CHECK ((((f)::integer > 1) AND (g > 2)));"
                        + "zzsd_k7=CHECK ((h > 1))",
                scalar("SELECT string_agg(conname||'='||pg_get_constraintdef(oid), ';'"
                        + " ORDER BY conname) FROM pg_constraint"
                        + " WHERE conrelid = 'zzsd_m'::regclass"));
        assertEquals("a/bigint,b/numeric,c/zzsd_pos,d/zzsd_pos,e/zzsd_pos,f/zzsd_pos,g/integer,"
                        + "h/smallint",
                scalar("SELECT string_agg(attname||'/'||format_type(atttypid, atttypmod), ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzsd_m'::regclass AND attnum > 0"));

        // Both rules are enforced throughout: the domain's own, and then the relation's.
        exec("INSERT INTO zzsd_m VALUES (2, 2, 2, 2, 2, 2, 3, 2)");
        assertEquals("value for domain zzsd_pos violates check constraint \"zzsd_pos_check\"",
                messageOf("INSERT INTO zzsd_m VALUES (2, 2, 0, 2, 2, 2, 3, 2)"));
        assertEquals("new row for relation \"zzsd_m\" violates check constraint \"zzsd_k3\"",
                messageOf("INSERT INTO zzsd_m VALUES (2, 2, 1, 2, 2, 2, 3, 2)"));

        // And the reading down goes again when the column stops being a domain.
        exec("ALTER TABLE zzsd_m ALTER COLUMN c TYPE int");
        assertEquals("CHECK ((c > 1))", scalar("SELECT pg_get_constraintdef(oid)"
                + " FROM pg_constraint WHERE conname = 'zzsd_k3'"));

        exec("DROP TABLE zzsd_m");
        exec("DROP DOMAIN zzsd_pos");
    }

    // ------------------------------------------------------------ A partition key element that opens a query

    @Test
    void aKeyElementWrittenWithOnePairOfParenthesesStopsAtTheWordThatOpensIt() throws Exception {
        // The pair of parentheses a sub-query is written with is the pair the key element has
        // already spent, so the word that opens the query is a syntax error where it stands.
        String[][] refused = {
                {"CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE ((SELECT 1))",
                        "syntax error at or near \"SELECT\""},
                {"CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE (SELECT 1)",
                        "syntax error at or near \"SELECT\""},
                {"CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE (i, (SELECT 1))",
                        "syntax error at or near \"SELECT\""},
                {"CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE ((SELECT 1), i)",
                        "syntax error at or near \"SELECT\""},
                {"CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE ((TABLE zzsd_pk))",
                        "syntax error at or near \"TABLE\""},
                {"CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE"
                        + " ((WITH q AS (SELECT 1) SELECT 1 FROM q))",
                        "syntax error at or near \"WITH\""},
        };
        for (String[] one : refused) {
            assertEquals("42601", stateOf(one[0]), one[0]);
            assertEquals(one[1], messageOf(one[0]), one[0]);
        }

        // Two pairs, and the element is an expression the analysis reaches and refuses for what
        // it is -- under every strategy.
        String[] doubled = {
                "CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE (((SELECT 1)))",
                "CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE (((SELECT 1) + 1))",
                "CREATE TABLE zzsd_pk (i int, k int) PARTITION BY LIST (((SELECT 1)))",
                "CREATE TABLE zzsd_pk (i int, k int) PARTITION BY HASH (((SELECT 1)))",
        };
        for (String sql : doubled) {
            assertEquals("0A000", stateOf(sql), sql);
            assertEquals("cannot use subquery in partition key expression", messageOf(sql), sql);
        }

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_pk'"));
    }

    @Test
    void theFaultAKeyExpressionIsRefusedForIsWhicheverTheAnalysisReachesFirst() throws Exception {
        // PostgreSQL analyses a key expression from the leaves outwards, exactly as it analyses
        // one written in a query.
        String[][] refused = {
                {"((sum(i) + nosuch))", "42803",
                        "aggregate functions are not allowed in partition key expressions"},
                {"((nosuch + sum(i)))", "42703", "column \"nosuch\" does not exist"},
                {"((sum(i) + nosuch), nosuch2)", "42803",
                        "aggregate functions are not allowed in partition key expressions"},
                {"((row_number() OVER () + nosuch))", "42P20",
                        "window functions are not allowed in partition key expressions"},
                {"((row_number() OVER (ORDER BY nosuch)))", "42P20",
                        "window functions are not allowed in partition key expressions"},
                {"(((SELECT 1) + nosuch))", "0A000",
                        "cannot use subquery in partition key expression"},
                {"((nosuch + (SELECT 1)))", "42703", "column \"nosuch\" does not exist"},
                // A call's arguments are analysed before the call is placed.
                {"((sum(nosuch)))", "42703", "column \"nosuch\" does not exist"},
                {"((abs(i) FILTER (WHERE nosuch)))", "42703", "column \"nosuch\" does not exist"},
                {"((CASE WHEN nosuch THEN sum(i) END))", "42703",
                        "column \"nosuch\" does not exist"},
                {"((CASE WHEN true THEN sum(i) ELSE nosuch END))", "42803",
                        "aggregate functions are not allowed in partition key expressions"},
                {"((nosuch + nosuch2))", "42703", "column \"nosuch\" does not exist"},
                // A system column resolves like any other, so the name that is not there is the
                // fault wherever it stands beside one.
                {"((xmin + nosuch))", "42703", "column \"nosuch\" does not exist"},
                {"((nosuch + xmin))", "42703", "column \"nosuch\" does not exist"},
                // ...and a name is reached before the key is judged for changing its answer.
                {"((random()::int + nosuch))", "42703", "column \"nosuch\" does not exist"},
        };
        for (String[] one : refused) {
            String sql = "CREATE TABLE zzsd_pk (i int, k int) PARTITION BY RANGE " + one[0];
            assertEquals(one[1], stateOf(sql), sql);
            assertEquals(one[2], messageOf(sql), sql);
        }

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzsd_pk'"));
    }

    @Test
    void everyExpressionAQueryMayHoldMayAlsoStandInAPartitionKey() throws Exception {
        String[] keys = {
                "RANGE (i)", "RANGE ((i))", "RANGE ((i + k))", "LIST (lower(s))", "RANGE (abs(i))",
                "RANGE (i, k)", "RANGE ((CASE WHEN i > 0 THEN 1 ELSE 2 END))", "LIST ((s || 'x'))",
                "RANGE ((i::bigint))", "HASH ((i + 1))", "RANGE ((i BETWEEN 1 AND 9))",
                "LIST ((i IN (1, 2)))", "RANGE ((greatest(i, k)))",
        };
        for (int n = 0; n < keys.length; n++) {
            exec("CREATE TABLE zzsd_key" + n + " (i int, k int, s text) PARTITION BY " + keys[n]);
        }
        // ...and a subscript of an array column, which needs a column of its own.
        exec("CREATE TABLE zzsd_keyx (i int, a int[], s text) PARTITION BY RANGE ((a[1]))");

        assertEquals(keys.length + 1, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname LIKE 'zzsd\\_key%' AND relkind = 'p'"));
        assertEquals("RANGE (i)", scalar("SELECT pg_get_partkeydef('zzsd_key0'::regclass)"));
        assertEquals("LIST (lower(s))", scalar("SELECT pg_get_partkeydef('zzsd_key3'::regclass)"));
        assertEquals("RANGE (abs(i))", scalar("SELECT pg_get_partkeydef('zzsd_key4'::regclass)"));
        assertEquals("RANGE (i, k)", scalar("SELECT pg_get_partkeydef('zzsd_key5'::regclass)"));

        for (int n = 0; n < keys.length; n++) exec("DROP TABLE zzsd_key" + n);
        exec("DROP TABLE zzsd_keyx");
    }

    // ------------------------------------------------------------ What a child may generate that its parent does not

    @Test
    void attachingAPartitionAsksThatAColumnBeGeneratedOnBothSidesOrOnNeither() throws Exception {
        exec("CREATE TABLE zzsd_gp (i int, k int, s text) PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)");
        assertEquals("42804",
                stateOf("ALTER TABLE zzsd_gp ATTACH PARTITION zzsd_gc FOR VALUES IN ('a')"));
        assertEquals("column \"k\" in child table must not be a generated column",
                messageOf("ALTER TABLE zzsd_gp ATTACH PARTITION zzsd_gc FOR VALUES IN ('a')"));
        exec("DROP TABLE zzsd_gc");
        exec("DROP TABLE zzsd_gp");

        exec("CREATE TABLE zzsd_gp (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)"
                + " PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_gc (i int, k int, s text)");
        assertEquals("column \"k\" in child table must be a generated column",
                messageOf("ALTER TABLE zzsd_gp ATTACH PARTITION zzsd_gc FOR VALUES IN ('a')"));
        exec("DROP TABLE zzsd_gc");
        exec("DROP TABLE zzsd_gp");

        // A default is not a generation expression.
        exec("CREATE TABLE zzsd_gp (i int, k int DEFAULT 7, s text) PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)");
        assertEquals("column \"k\" in child table must not be a generated column",
                messageOf("ALTER TABLE zzsd_gp ATTACH PARTITION zzsd_gc FOR VALUES IN ('a')"));
        exec("DROP TABLE zzsd_gc");
        exec("DROP TABLE zzsd_gp");

        // Two generated columns of different kinds are refused for the kind, with both named.
        exec("CREATE TABLE zzsd_gp (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL, s text)"
                + " PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)");
        org.postgresql.util.ServerErrorMessage e =
                fieldsOf("ALTER TABLE zzsd_gp ATTACH PARTITION zzsd_gc FOR VALUES IN ('a')");
        assertEquals("42804", e.getSQLState());
        assertEquals("column \"k\" inherits from generated column of different kind", e.getMessage());
        assertEquals("Parent column is VIRTUAL, child column is STORED.", e.getDetail());
        exec("DROP TABLE zzsd_gc");
        exec("DROP TABLE zzsd_gp");
    }

    @Test
    void twoGeneratedColumnsOfTheSameKindMayDisagreeAboutTheExpression() throws Exception {
        exec("CREATE TABLE zzsd_gp (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)"
                + " PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_gc (i int, k int GENERATED ALWAYS AS (i * 3) STORED, s text)");
        exec("ALTER TABLE zzsd_gp ATTACH PARTITION zzsd_gc FOR VALUES IN ('a')");
        assertEquals("(i * 3)", scalar("SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef"
                + " WHERE adrelid = 'zzsd_gc'::regclass"));
        exec("DROP TABLE zzsd_gp CASCADE");

        // Two virtual ones of the same kind go the same way, and so do two plain columns and two
        // columns carrying a DEFAULT, which is not inherited at all.
        exec("CREATE TABLE zzsd_vp (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL, s text)"
                + " PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_vc (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL, s text)");
        exec("ALTER TABLE zzsd_vp ATTACH PARTITION zzsd_vc FOR VALUES IN ('a')");
        assertEquals(1, num("SELECT count(*)::int FROM pg_inherits"
                + " WHERE inhrelid = 'zzsd_vc'::regclass"));
        exec("DROP TABLE zzsd_vp CASCADE");

        exec("CREATE TABLE zzsd_np (i int, k int, s text) PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_nc (i int, k int, s text)");
        exec("ALTER TABLE zzsd_np ATTACH PARTITION zzsd_nc FOR VALUES IN ('a')");
        exec("INSERT INTO zzsd_np VALUES (1, 2, 'a')");
        assertEquals("1/2/a", rowsOf("SELECT i, k, s FROM zzsd_nc"));
        exec("DROP TABLE zzsd_np CASCADE");

        exec("CREATE TABLE zzsd_dp (i int, k int DEFAULT 7, s text) PARTITION BY LIST (s)");
        exec("CREATE TABLE zzsd_dc2 (i int, k int DEFAULT 9, s text)");
        exec("ALTER TABLE zzsd_dp ATTACH PARTITION zzsd_dc2 FOR VALUES IN ('a')");
        assertEquals(1, num("SELECT count(*)::int FROM pg_inherits"
                + " WHERE inhrelid = 'zzsd_dc2'::regclass"));
        exec("DROP TABLE zzsd_dp CASCADE");
    }

    @Test
    void joiningAnInheritanceHierarchyAsksTheSameOfAGeneratedColumn() throws Exception {
        exec("CREATE TABLE zzsd_ip (i int, k int, s text)");
        exec("CREATE TABLE zzsd_ic (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)");
        assertEquals("42804", stateOf("ALTER TABLE zzsd_ic INHERIT zzsd_ip"));
        assertEquals("column \"k\" in child table must not be a generated column",
                messageOf("ALTER TABLE zzsd_ic INHERIT zzsd_ip"));
        exec("DROP TABLE zzsd_ic");
        exec("DROP TABLE zzsd_ip");

        exec("CREATE TABLE zzsd_ip (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)");
        exec("CREATE TABLE zzsd_ic (i int, k int, s text)");
        assertEquals("column \"k\" in child table must be a generated column",
                messageOf("ALTER TABLE zzsd_ic INHERIT zzsd_ip"));
        exec("DROP TABLE zzsd_ic");
        exec("DROP TABLE zzsd_ip");

        exec("CREATE TABLE zzsd_ip (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)");
        exec("CREATE TABLE zzsd_ic (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text)");
        exec("ALTER TABLE zzsd_ic INHERIT zzsd_ip");
        assertEquals(1, num("SELECT count(*)::int FROM pg_inherits"
                + " WHERE inhrelid = 'zzsd_ic'::regclass"));
        exec("ALTER TABLE zzsd_ic NO INHERIT zzsd_ip");
        exec("DROP TABLE zzsd_ic");
        exec("DROP TABLE zzsd_ip");
    }

    // ------------------------------------------------------------ Where the policies and the check option sit

    @Test
    void aViewsCheckOptionIsAskedAfterTheRowIsStoredAndAfterTheArbiter() throws Exception {
        exec("CREATE TABLE zzsd_vt (i int PRIMARY KEY, v int CHECK (v < 100),"
                + " w int NOT NULL DEFAULT 1)");
        exec("INSERT INTO zzsd_vt (i, v) VALUES (60, 1), (70, 1)");
        exec("CREATE VIEW zzsd_vv AS SELECT * FROM zzsd_vt WHERE v < 10 WITH CHECK OPTION");

        // A row that duplicates a key is refused for the key, whether or not the view would have
        // taken it; a row that breaks a CHECK is refused for the CHECK.
        assertEquals("23505", stateOf("INSERT INTO zzsd_vv (i, v) VALUES (60, 20)"));
        assertEquals("23505", stateOf("INSERT INTO zzsd_vv (i, v) VALUES (60, 5)"));
        assertEquals("23514", stateOf("INSERT INTO zzsd_vv (i, v) VALUES (30, 200)"));
        assertEquals("23502", stateOf("INSERT INTO zzsd_vv (i, v, w) VALUES (30, 20, NULL)"));
        assertEquals("44000", stateOf("INSERT INTO zzsd_vv (i, v) VALUES (30, 20)"));
        assertEquals("new row violates check option for view \"zzsd_vv\"",
                messageOf("INSERT INTO zzsd_vv (i, v) VALUES (30, 20)"));

        assertEquals("23505", stateOf("UPDATE zzsd_vv SET i = 60, v = 20 WHERE i = 70"));
        assertEquals("23514", stateOf("UPDATE zzsd_vv SET i = 80, v = 200 WHERE i = 70"));
        assertEquals("44000", stateOf("UPDATE zzsd_vv SET v = 20 WHERE i = 70"));

        // The arbiter comes between: a row it skips is never offered to the view.
        exec("INSERT INTO zzsd_vv (i, v) VALUES (60, 20) ON CONFLICT (i) DO NOTHING");
        assertEquals("44000",
                stateOf("INSERT INTO zzsd_vv (i, v) VALUES (30, 20) ON CONFLICT (i) DO NOTHING"));
        assertEquals("23514",
                stateOf("INSERT INTO zzsd_vv (i, v) VALUES (60, 200) ON CONFLICT (i) DO NOTHING"));
        // ...and the row a conflict clause leaves behind is a row of the view like any other.
        assertEquals("44000", stateOf("INSERT INTO zzsd_vv (i, v) VALUES (60, 20)"
                + " ON CONFLICT (i) DO UPDATE SET v = 30"));

        // A row the view does admit is written.
        exec("INSERT INTO zzsd_vv (i, v) VALUES (30, 5) ON CONFLICT (i) DO NOTHING");
        assertEquals("30/5/1;60/1/1;70/1/1", rowsOf("SELECT i, v, w FROM zzsd_vt ORDER BY i"));

        exec("DROP VIEW zzsd_vv");
        exec("DROP TABLE zzsd_vt");
    }

    @Test
    void theRowLevelPoliciesAreAskedBeforeTheArbiterAndBeforeTheConstraints() throws Exception {
        exec("CREATE TABLE zzsd_rl (i int PRIMARY KEY, v int CHECK (v < 100),"
                + " w int NOT NULL DEFAULT 1)");
        exec("INSERT INTO zzsd_rl (i, v) VALUES (60, 1), (70, 1)");
        exec("ALTER TABLE zzsd_rl ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE zzsd_rl FORCE ROW LEVEL SECURITY");
        exec("CREATE POLICY zzsd_pol ON zzsd_rl FOR ALL USING (true) WITH CHECK (i < 50)");
        exec("DROP ROLE IF EXISTS zzsd_user");
        exec("CREATE ROLE zzsd_user LOGIN");
        exec("GRANT ALL ON zzsd_rl TO zzsd_user");
        exec("SET ROLE zzsd_user");
        try {
            // A row no policy admits is refused even where the arbiter would have skipped it, and
            // even where a column it holds is one the relation would have refused anyway.
            String[] refused = {
                    "INSERT INTO zzsd_rl (i, v) VALUES (60, 1) ON CONFLICT (i) DO NOTHING",
                    "INSERT INTO zzsd_rl (i, v) VALUES (60, 200) ON CONFLICT (i) DO NOTHING",
                    "INSERT INTO zzsd_rl (i, v, w) VALUES (60, 1, NULL) ON CONFLICT (i) DO NOTHING",
                    "INSERT INTO zzsd_rl (i, v) VALUES (60, 1) ON CONFLICT (i) DO UPDATE SET v = 2",
                    "INSERT INTO zzsd_rl (i, v) VALUES (80, 200)",
                    "INSERT INTO zzsd_rl (i, v, w) VALUES (80, 1, NULL)",
                    "UPDATE zzsd_rl SET i = 80, v = 200 WHERE i = 70",
            };
            for (String sql : refused) {
                assertEquals("42501", stateOf(sql), sql);
                assertEquals("new row violates row-level security policy for table \"zzsd_rl\"",
                        messageOf(sql), sql);
            }

            // A row the policies do admit is judged against the relation's own rules next.
            assertEquals("23514",
                    stateOf("INSERT INTO zzsd_rl (i, v) VALUES (40, 200)"
                            + " ON CONFLICT (i) DO NOTHING"));
            assertEquals("new row for relation \"zzsd_rl\" violates check constraint"
                            + " \"zzsd_rl_v_check\"",
                    messageOf("INSERT INTO zzsd_rl (i, v) VALUES (40, 200)"
                            + " ON CONFLICT (i) DO NOTHING"));
            exec("INSERT INTO zzsd_rl (i, v) VALUES (40, 5) ON CONFLICT (i) DO NOTHING");
            assertEquals("40/5/1;60/1/1;70/1/1", rowsOf("SELECT i, v, w FROM zzsd_rl ORDER BY i"));
        } finally {
            exec("RESET ROLE");
        }
        exec("DROP TABLE zzsd_rl CASCADE");
        exec("DROP ROLE zzsd_user");
    }

    // ------------------------------------------------------------ What counts as writing to NEW

    @Test
    void anExecuteIntoAFieldOfNewAndAWholeRowAssignedToNewAreBothRewrites() throws Exception {
        // A copy of a partitioned table's row trigger may not carry the row out of the partition
        // the insert was routed to, and PostgreSQL decides whether the copy rewrote the row by
        // whether the routine handed back a tuple other than the one it was given.
        exec("CREATE FUNCTION zzsd_tg_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := NEW.k + 10; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzsd_tg_exi() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " EXECUTE 'SELECT $1' INTO NEW.i USING NEW.i; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzsd_tg_exrec() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " EXECUTE 'SELECT $1, $2, $3' INTO NEW USING NEW.i, NEW.k, NEW.a;"
                + " RETURN NEW; END $$");
        exec("CREATE FUNCTION zzsd_tg_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW := ROW(NEW.i, NEW.k, NEW.a); RETURN NEW; END $$");
        exec("CREATE FUNCTION zzsd_tg_self() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW := NEW; RETURN NEW; END $$");
        exec("CREATE TABLE zzsd_tq (i int, k int, a int[]) PARTITION BY RANGE (k)");
        exec("CREATE TABLE zzsd_tq0 PARTITION OF zzsd_tq FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzsd_tq1 PARTITION OF zzsd_tq FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER zzsd_tg_a_move BEFORE INSERT ON zzsd_tq0 FOR EACH ROW"
                + " EXECUTE FUNCTION zzsd_tg_bump()");

        // Writing a field of NEW through EXECUTE, writing the whole record through EXECUTE, and
        // assigning a whole row to NEW are each a rewrite.
        assertEquals("0A000", newWriteVerdict("zzsd_tg_exi"));
        assertEquals("0A000", newWriteVerdict("zzsd_tg_exrec"));
        assertEquals("0A000", newWriteVerdict("zzsd_tg_row"));
        // Handing PostgreSQL back the record it gave is not, so the row reaches the partition
        // constraint the trigger declared on the partition moved it out of.
        assertEquals("23514", newWriteVerdict("zzsd_tg_self"));

        assertEquals(0, num("SELECT count(*)::int FROM zzsd_tq"));
        exec("DROP TABLE zzsd_tq CASCADE");
        for (String routine : new String[] {"zzsd_tg_bump", "zzsd_tg_exi", "zzsd_tg_exrec",
                "zzsd_tg_row", "zzsd_tg_self"}) {
            exec("DROP FUNCTION " + routine + "() CASCADE");
        }
    }

    /** Fires one routine as a copy of the partitioned table's trigger and reads the verdict back. */
    private static String newWriteVerdict(String routine) throws SQLException {
        exec("CREATE TRIGGER zzsd_tg_b_x BEFORE INSERT ON zzsd_tq FOR EACH ROW"
                + " EXECUTE FUNCTION " + routine + "()");
        String state = stateOf("INSERT INTO zzsd_tq VALUES (1, 5, '{1,2}')");
        exec("DROP TRIGGER zzsd_tg_b_x ON zzsd_tq");
        return state;
    }

    @Test
    void bothFormsRunAndLeaveTheRowAsTheyFoundItOnAnOrdinaryRelation() throws Exception {
        exec("CREATE FUNCTION zzsd_og_exi() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " EXECUTE 'SELECT $1' INTO NEW.i USING NEW.i; RETURN NEW; END $$");
        exec("CREATE FUNCTION zzsd_og_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW := ROW(NEW.i, NEW.k, NEW.a); RETURN NEW; END $$");
        exec("CREATE FUNCTION zzsd_og_exrec() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " EXECUTE 'SELECT $1, $2, $3' INTO NEW USING NEW.i, NEW.k, NEW.a;"
                + " RETURN NEW; END $$");
        exec("CREATE TABLE zzsd_ot (i int, k int, a int[])");

        exec("CREATE TRIGGER zzsd_ox BEFORE INSERT ON zzsd_ot FOR EACH ROW"
                + " EXECUTE FUNCTION zzsd_og_exi()");
        exec("INSERT INTO zzsd_ot VALUES (1, 5, '{1,2}')");
        assertEquals("1/5/{1,2}", rowsOf("SELECT i, k, a FROM zzsd_ot"));
        exec("DROP TRIGGER zzsd_ox ON zzsd_ot");
        exec("DELETE FROM zzsd_ot");

        exec("CREATE TRIGGER zzsd_ox BEFORE INSERT ON zzsd_ot FOR EACH ROW"
                + " EXECUTE FUNCTION zzsd_og_row()");
        exec("INSERT INTO zzsd_ot VALUES (2, 6, '{3,4}')");
        assertEquals("2/6/{3,4}", rowsOf("SELECT i, k, a FROM zzsd_ot"));
        exec("DROP TRIGGER zzsd_ox ON zzsd_ot");
        exec("DELETE FROM zzsd_ot");

        exec("CREATE TRIGGER zzsd_ox BEFORE INSERT ON zzsd_ot FOR EACH ROW"
                + " EXECUTE FUNCTION zzsd_og_exrec()");
        exec("INSERT INTO zzsd_ot VALUES (3, 7, '{5,6}')");
        assertEquals("3/7/{5,6}", rowsOf("SELECT i, k, a FROM zzsd_ot"));

        exec("DROP TABLE zzsd_ot CASCADE");
        exec("DROP FUNCTION zzsd_og_exi() CASCADE");
        exec("DROP FUNCTION zzsd_og_row() CASCADE");
        exec("DROP FUNCTION zzsd_og_exrec() CASCADE");
    }

    // ------------------------------------------------------------ Every name one DROP is given is settled before any of them is taken

    /**
     * Every notice the statement raised, each as its message followed by the lines of its DETAIL
     * in brackets. A cascade that took more than one object sends only a count in the message and
     * the list itself in DETAIL, so the two have to be read together.
     */
    private static List<String> noticesOf(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            for (SQLWarning w = st.getWarnings(); w != null; w = w.getNextWarning()) {
                StringBuilder sb = new StringBuilder(w.getMessage().trim());
                if (w instanceof org.postgresql.util.PSQLWarning) {
                    org.postgresql.util.ServerErrorMessage m =
                            ((org.postgresql.util.PSQLWarning) w).getServerErrorMessage();
                    if (m != null && m.getDetail() != null) {
                        sb.append(" [").append(m.getDetail().replace("\n", " | ")).append(']');
                    }
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    /**
     * PostgreSQL settles the whole list of names before it takes any of it, so what a CASCADE
     * reports is only what the statement did not itself name. A relation another name in the same
     * list carries off is going anyway, and is neither reported as a cascade nor missed when its
     * own turn comes.
     */
    @Test
    void aCascadeReportsNothingTheSameStatementNamed() throws Exception {
        // A table a foreign key points at, taken down beside the table that points at it.
        exec("CREATE TABLE zzr7gn_np (i int PRIMARY KEY)");
        exec("CREATE TABLE zzr7gn_nc (i int REFERENCES zzr7gn_np(i))");
        assertEquals(List.of(), noticesOf("DROP TABLE zzr7gn_np, zzr7gn_nc CASCADE"));

        // An inheritance child the same DROP names.
        exec("CREATE TABLE zzr7gn_npa (i int)");
        exec("CREATE TABLE zzr7gn_nch (j int) INHERITS (zzr7gn_npa)");
        assertEquals(List.of(), noticesOf("DROP TABLE zzr7gn_npa, zzr7gn_nch CASCADE"));

        // A view over a view, both named.
        exec("CREATE TABLE zzr7gn_nt (i int)");
        exec("CREATE VIEW zzr7gn_nv1 AS SELECT * FROM zzr7gn_nt");
        exec("CREATE VIEW zzr7gn_nv2 AS SELECT * FROM zzr7gn_nv1");
        assertEquals(List.of(), noticesOf("DROP VIEW zzr7gn_nv1, zzr7gn_nv2 CASCADE"));

        // A materialized view over a materialized view, both named.
        exec("CREATE MATERIALIZED VIEW zzr7gn_nm1 AS SELECT * FROM zzr7gn_nt");
        exec("CREATE MATERIALIZED VIEW zzr7gn_nm2 AS SELECT * FROM zzr7gn_nm1");
        assertEquals(List.of(),
                noticesOf("DROP MATERIALIZED VIEW zzr7gn_nm1, zzr7gn_nm2 CASCADE"));

        // A partition of a partitioned table the list also names, with a reader it does not:
        // only the reader is reported.
        exec("CREATE TABLE zzr7gn_nhq (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzr7gn_nh1 PARTITION OF zzr7gn_nhq FOR VALUES FROM (0) TO (10)");
        exec("CREATE VIEW zzr7gn_npv AS SELECT * FROM zzr7gn_nh1");
        assertEquals(List.of("drop cascades to view zzr7gn_npv"),
                noticesOf("DROP TABLE zzr7gn_nhq, zzr7gn_nh1 CASCADE"));

        // IF EXISTS has nothing to skip: both names were there when the list was read.
        exec("CREATE TABLE zzr7gn_nhq (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzr7gn_nh1 PARTITION OF zzr7gn_nhq FOR VALUES FROM (0) TO (10)");
        assertEquals(List.of(), noticesOf("DROP TABLE IF EXISTS zzr7gn_nhq, zzr7gn_nh1"));

        exec("DROP TABLE zzr7gn_nt");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * A cascade names each object it reached once, however many of the statement's names reach
     * it. Where it took more than one, the message counts them and the DETAIL lists them.
     */
    @Test
    void aCascadeNamesWhatItReachedOnceHoweverManyWaysItReachesIt() throws Exception {
        // One view reading both tables the statement names is reported once.
        exec("CREATE TABLE zzr7gn_x1 (i int)");
        exec("CREATE TABLE zzr7gn_x2 (i int)");
        exec("CREATE VIEW zzr7gn_xv AS SELECT a.i FROM zzr7gn_x1 a JOIN zzr7gn_x2 b ON a.i = b.i");
        assertEquals(List.of("drop cascades to view zzr7gn_xv"),
                noticesOf("DROP TABLE zzr7gn_x1, zzr7gn_x2 CASCADE"));

        // A view reached both down a chain and straight from the second table is reported once,
        // and two objects taken are counted in the message and listed in DETAIL.
        exec("CREATE TABLE zzr7gn_y1 (i int)");
        exec("CREATE TABLE zzr7gn_y2 (i int)");
        exec("CREATE VIEW zzr7gn_w1 AS SELECT * FROM zzr7gn_y1");
        exec("CREATE VIEW zzr7gn_w2 AS SELECT a.i FROM zzr7gn_w1 a JOIN zzr7gn_y2 b"
                + " ON a.i = b.i");
        assertEquals(List.of("drop cascades to 2 other objects"
                        + " [drop cascades to view zzr7gn_w1 | drop cascades to view zzr7gn_w2]"),
                noticesOf("DROP TABLE zzr7gn_y1, zzr7gn_y2 CASCADE"));

        // The same shape from a single name, so the count and the DETAIL are not a list artefact.
        exec("CREATE TABLE zzr7gn_z1 (i int)");
        exec("CREATE VIEW zzr7gn_zv1 AS SELECT * FROM zzr7gn_z1");
        exec("CREATE VIEW zzr7gn_zv2 AS SELECT * FROM zzr7gn_zv1");
        assertEquals(List.of("drop cascades to 2 other objects"
                        + " [drop cascades to view zzr7gn_zv1 | drop cascades to view zzr7gn_zv2]"),
                noticesOf("DROP TABLE zzr7gn_z1 CASCADE"));

        // A materialized view is named by the kind it is, and a foreign key by its constraint.
        exec("CREATE TABLE zzr7gn_mt (i int)");
        exec("CREATE MATERIALIZED VIEW zzr7gn_mv AS SELECT * FROM zzr7gn_mt");
        assertEquals(List.of("drop cascades to materialized view zzr7gn_mv"),
                noticesOf("DROP TABLE zzr7gn_mt CASCADE"));
        exec("CREATE TABLE zzr7gn_ep (i int PRIMARY KEY)");
        exec("CREATE TABLE zzr7gn_ec (i int REFERENCES zzr7gn_ep(i))");
        assertEquals(List.of("drop cascades to constraint zzr7gn_ec_i_fkey"
                        + " on table zzr7gn_ec"),
                noticesOf("DROP TABLE zzr7gn_ep CASCADE"));
        exec("DROP TABLE zzr7gn_ec");

        // A sequence a column owns names the default the column loses.
        exec("CREATE TABLE zzr7gn_sq (i serial, j int)");
        assertEquals(List.of("drop cascades to default value for column i of table zzr7gn_sq"),
                noticesOf("DROP SEQUENCE zzr7gn_sq_i_seq CASCADE"));
        exec("DROP TABLE zzr7gn_sq");

        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * A refusal names the relation the dependency really hangs from, which need not be the name
     * the statement wrote; and a statement that named several says only that it cannot have what
     * it asked for.
     */
    @Test
    void aRefusalNamesTheRelationTheDependencyReallyHangsFrom() throws Exception {
        exec("CREATE TABLE zzr7gn_bq (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzr7gn_b1 PARTITION OF zzr7gn_bq FOR VALUES FROM (0) TO (10)");
        exec("CREATE VIEW zzr7gn_bv AS SELECT * FROM zzr7gn_b1");

        // The partition goes because the statement named the table it belongs to, so the refusal
        // is written against the name in the statement and the DETAIL against the partition.
        org.postgresql.util.ServerErrorMessage one = fieldsOf("DROP TABLE zzr7gn_bq");
        assertEquals("2BP01", one.getSQLState());
        assertEquals("cannot drop table zzr7gn_bq because other objects depend on it",
                one.getMessage());
        assertEquals("view zzr7gn_bv depends on table zzr7gn_b1", one.getDetail());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", one.getHint());

        org.postgresql.util.ServerErrorMessage both =
                fieldsOf("DROP TABLE zzr7gn_bq, zzr7gn_b1");
        assertEquals("2BP01", both.getSQLState());
        assertEquals("cannot drop desired object(s) because other objects depend on them",
                both.getMessage());
        assertEquals("view zzr7gn_bv depends on table zzr7gn_b1", both.getDetail());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", both.getHint());

        exec("DROP TABLE zzr7gn_bq, zzr7gn_b1 CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('zzr7gn_bq','zzr7gn_b1','zzr7gn_bv')"));

        // A reader of two names in the list is a dependency of the first of them, once.
        exec("CREATE TABLE zzr7gn_x1 (i int)");
        exec("CREATE TABLE zzr7gn_x2 (i int)");
        exec("CREATE VIEW zzr7gn_xv AS SELECT a.i FROM zzr7gn_x1 a JOIN zzr7gn_x2 b ON a.i = b.i");
        assertEquals("view zzr7gn_xv depends on table zzr7gn_x1",
                detailOf("DROP TABLE zzr7gn_x1, zzr7gn_x2"));
        exec("DROP TABLE zzr7gn_x1, zzr7gn_x2 CASCADE");

        // A foreign key is named as the constraint it is, on the table that declared it.
        exec("CREATE TABLE zzr7gn_ep (i int PRIMARY KEY)");
        exec("CREATE TABLE zzr7gn_ec (i int REFERENCES zzr7gn_ep(i))");
        exec("CREATE TABLE zzr7gn_eo (i int)");
        assertEquals("cannot drop desired object(s) because other objects depend on them",
                messageOf("DROP TABLE zzr7gn_ep, zzr7gn_eo"));
        assertEquals("constraint zzr7gn_ec_i_fkey on table zzr7gn_ec depends on table zzr7gn_ep",
                detailOf("DROP TABLE zzr7gn_ep, zzr7gn_eo"));
        assertEquals(3, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('zzr7gn_ep','zzr7gn_ec','zzr7gn_eo')"));
        exec("DROP TABLE zzr7gn_ec, zzr7gn_ep, zzr7gn_eo");

        // A sequence a column owns is held by the default that column carries.
        exec("CREATE TABLE zzr7gn_sq (i serial, j int)");
        org.postgresql.util.ServerErrorMessage seq = fieldsOf("DROP SEQUENCE zzr7gn_sq_i_seq");
        assertEquals("2BP01", seq.getSQLState());
        assertEquals("cannot drop sequence zzr7gn_sq_i_seq because other objects depend on it",
                seq.getMessage());
        assertEquals("default value for column i of table zzr7gn_sq"
                + " depends on sequence zzr7gn_sq_i_seq", seq.getDetail());
        exec("DROP TABLE zzr7gn_sq");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * A name of a kind the statement was not written for is refused while the list is being read,
     * before anything goes, with the hint that name's own kind earns. IF EXISTS is no excuse: the
     * name is there, it is simply not of the kind asked for.
     */
    @Test
    void aNameOfTheWrongKindIsRefusedWithTheHintItsOwnKindEarns() throws Exception {
        exec("CREATE TABLE zzr7gn_kt (i int)");
        exec("CREATE VIEW zzr7gn_kv AS SELECT * FROM zzr7gn_kt");
        exec("CREATE INDEX zzr7gn_ki ON zzr7gn_kt (i)");
        exec("CREATE TABLE zzr7gn_ks (i serial, j int)");

        org.postgresql.util.ServerErrorMessage view = fieldsOf("DROP TABLE zzr7gn_kt, zzr7gn_kv");
        assertEquals("42809", view.getSQLState());
        assertEquals("\"zzr7gn_kv\" is not a table", view.getMessage());
        assertEquals("Use DROP VIEW to remove a view.", view.getHint());
        assertNull(view.getDetail());

        org.postgresql.util.ServerErrorMessage table = fieldsOf("DROP VIEW zzr7gn_kv, zzr7gn_kt");
        assertEquals("42809", table.getSQLState());
        assertEquals("\"zzr7gn_kt\" is not a view", table.getMessage());
        assertEquals("Use DROP TABLE to remove a table.", table.getHint());

        org.postgresql.util.ServerErrorMessage index = fieldsOf("DROP TABLE zzr7gn_kt, zzr7gn_ki");
        assertEquals("42809", index.getSQLState());
        assertEquals("\"zzr7gn_ki\" is not a table", index.getMessage());
        assertEquals("Use DROP INDEX to remove an index.", index.getHint());

        assertEquals("\"zzr7gn_kt\" is not an index",
                messageOf("DROP INDEX zzr7gn_ki, zzr7gn_kt"));
        assertEquals("Use DROP TABLE to remove a table.",
                hintOf("DROP INDEX zzr7gn_ki, zzr7gn_kt"));

        org.postgresql.util.ServerErrorMessage sequence =
                fieldsOf("DROP TABLE zzr7gn_ks, zzr7gn_ks_i_seq");
        assertEquals("42809", sequence.getSQLState());
        assertEquals("\"zzr7gn_ks_i_seq\" is not a table", sequence.getMessage());
        assertEquals("Use DROP SEQUENCE to remove a sequence.", sequence.getHint());

        // IF EXISTS passes over a name that is not there; it does not pass over a name of the
        // wrong kind.
        assertEquals("42809", stateOf("DROP TABLE IF EXISTS zzr7gn_nosuch, zzr7gn_kv"));
        assertEquals("\"zzr7gn_kv\" is not a table",
                messageOf("DROP TABLE IF EXISTS zzr7gn_nosuch, zzr7gn_kv"));
        assertEquals("42809", stateOf("DROP VIEW IF EXISTS zzr7gn_kv, zzr7gn_nosuch,"
                + " zzr7gn_kt"));

        // Nothing went in any of them.
        assertEquals("zzr7gn_ki,zzr7gn_ks,zzr7gn_ks_i_seq,zzr7gn_kt,zzr7gn_kv",
                column("SELECT relname FROM pg_class WHERE relname LIKE 'zzr7gn\\_k%'"
                        + " ORDER BY 1"));

        exec("DROP TABLE zzr7gn_kt CASCADE");
        exec("DROP TABLE zzr7gn_ks");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * A name that was never there takes the whole statement with it, wherever in the list it
     * stands and whatever kind of object the statement asks for, and every other name is left
     * where it was.
     */
    @Test
    void aNameThatWasNeverThereLeavesEveryOtherNameWhereItWas() throws Exception {
        exec("CREATE TABLE zzr7gn_ga (i int, j int)");
        exec("CREATE VIEW zzr7gn_gv AS SELECT * FROM zzr7gn_ga");
        exec("CREATE SEQUENCE zzr7gn_gs");
        exec("CREATE INDEX zzr7gn_gi ON zzr7gn_ga (i)");
        exec("CREATE TYPE zzr7gn_ge AS ENUM ('a')");

        org.postgresql.util.ServerErrorMessage last =
                fieldsOf("DROP TABLE zzr7gn_ga, zzr7gn_nosuch");
        assertEquals("42P01", last.getSQLState());
        assertEquals("table \"zzr7gn_nosuch\" does not exist", last.getMessage());
        assertNull(last.getDetail());
        assertNull(last.getHint());

        // The same when it stands first, so the answer is not the first name the reader reached.
        assertEquals("42P01", stateOf("DROP TABLE zzr7gn_nosuch, zzr7gn_ga"));
        assertEquals("table \"zzr7gn_nosuch\" does not exist",
                messageOf("DROP TABLE zzr7gn_nosuch, zzr7gn_ga"));

        assertEquals("42P01", stateOf("DROP VIEW zzr7gn_gv, zzr7gn_nosuch"));
        assertEquals("view \"zzr7gn_nosuch\" does not exist",
                messageOf("DROP VIEW zzr7gn_gv, zzr7gn_nosuch"));

        assertEquals("42P01", stateOf("DROP SEQUENCE zzr7gn_gs, zzr7gn_nosuch"));
        assertEquals("sequence \"zzr7gn_nosuch\" does not exist",
                messageOf("DROP SEQUENCE zzr7gn_gs, zzr7gn_nosuch"));

        // An index and a type answer 42704 rather than 42P01, each about its own kind.
        assertEquals("42704", stateOf("DROP INDEX zzr7gn_gi, zzr7gn_nosuch"));
        assertEquals("index \"zzr7gn_nosuch\" does not exist",
                messageOf("DROP INDEX zzr7gn_gi, zzr7gn_nosuch"));

        assertEquals("42704", stateOf("DROP TYPE zzr7gn_ge, zzr7gn_nosuch"));
        assertEquals("type \"zzr7gn_nosuch\" does not exist",
                messageOf("DROP TYPE zzr7gn_ge, zzr7gn_nosuch"));

        assertEquals("zzr7gn_ga,zzr7gn_gi,zzr7gn_gs,zzr7gn_gv",
                column("SELECT relname FROM pg_class WHERE relname LIKE 'zzr7gn\\_g%'"
                        + " ORDER BY 1"));
        assertEquals(1, num("SELECT count(*) FROM pg_type WHERE typname = 'zzr7gn_ge'"));

        exec("DROP TABLE zzr7gn_ga CASCADE");
        exec("DROP SEQUENCE zzr7gn_gs");
        exec("DROP TYPE zzr7gn_ge");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * IF EXISTS passes over the name that was never there, says so in a notice naming the kind
     * the statement asked for, and takes every other name in the list.
     */
    @Test
    void ifExistsPassesOverTheNameThatWasNeverThereAndSaysSo() throws Exception {
        exec("CREATE TABLE zzr7gn_ia (i int)");
        assertEquals(List.of("table \"zzr7gn_nosuch\" does not exist, skipping"),
                noticesOf("DROP TABLE IF EXISTS zzr7gn_nosuch, zzr7gn_ia"));
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname = 'zzr7gn_ia'"));

        // Every name that was never there earns a notice of its own.
        assertEquals(List.of("table \"zzr7gn_nosuch\" does not exist, skipping",
                        "table \"zzr7gn_alsono\" does not exist, skipping"),
                noticesOf("DROP TABLE IF EXISTS zzr7gn_nosuch, zzr7gn_alsono"));

        // And each statement names the kind it was written for.
        assertEquals(List.of("view \"zzr7gn_nosuch\" does not exist, skipping"),
                noticesOf("DROP VIEW IF EXISTS zzr7gn_nosuch"));
        assertEquals(List.of("sequence \"zzr7gn_nosuch\" does not exist, skipping"),
                noticesOf("DROP SEQUENCE IF EXISTS zzr7gn_nosuch"));
        assertEquals(List.of("index \"zzr7gn_nosuch\" does not exist, skipping"),
                noticesOf("DROP INDEX IF EXISTS zzr7gn_nosuch"));
        assertEquals(List.of("type \"zzr7gn_nosuch\" does not exist, skipping"),
                noticesOf("DROP TYPE IF EXISTS zzr7gn_nosuch"));

        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * The whole DROP is one statement, so a transaction that takes it back puts every name it
     * was given back as it was: the partition still attached, the index still enforcing, the
     * enum still holding its labels and the sequence still counting from where it stood.
     */
    @Test
    void aRolledBackDropListPutsEveryNameBackAsItWas() throws Exception {
        exec("CREATE TABLE zzr7gn_ta (i int)");
        exec("CREATE TABLE zzr7gn_tb (i int)");
        exec("CREATE TABLE zzr7gn_hq (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzr7gn_h1 PARTITION OF zzr7gn_hq FOR VALUES FROM (0) TO (10)");
        exec("CREATE VIEW zzr7gn_v1 AS SELECT * FROM zzr7gn_ta");
        exec("CREATE VIEW zzr7gn_v2 AS SELECT * FROM zzr7gn_v1");
        exec("CREATE SEQUENCE zzr7gn_s1");
        exec("CREATE SEQUENCE zzr7gn_s2");
        exec("CREATE TABLE zzr7gn_ix (i int, j int)");
        exec("CREATE UNIQUE INDEX zzr7gn_i1 ON zzr7gn_ix (i)");
        exec("CREATE INDEX zzr7gn_i2 ON zzr7gn_ix (j)");
        exec("CREATE TYPE zzr7gn_e1 AS ENUM ('a','b')");
        exec("CREATE TYPE zzr7gn_e2 AS ENUM ('c')");

        rolledBack(() -> {
            exec("DROP VIEW zzr7gn_v1, zzr7gn_v2");
            exec("DROP TABLE zzr7gn_ta, zzr7gn_tb");
            exec("DROP TABLE zzr7gn_hq, zzr7gn_h1");
            exec("DROP SEQUENCE zzr7gn_s1, zzr7gn_s2");
            exec("DROP INDEX zzr7gn_i1, zzr7gn_i2");
            exec("DROP TYPE zzr7gn_e1, zzr7gn_e2");
            // Only the table the indexes were declared on is left standing.
            assertEquals(1, num("SELECT count(*) FROM pg_class"
                    + " WHERE relname LIKE 'zzr7gn\\_%'"));
            assertEquals(0, num("SELECT count(*) FROM pg_type"
                    + " WHERE typname IN ('zzr7gn_e1','zzr7gn_e2')"));
        });

        assertEquals("zzr7gn_h1,zzr7gn_hq,zzr7gn_i1,zzr7gn_i2,zzr7gn_ix,zzr7gn_s1,zzr7gn_s2,"
                        + "zzr7gn_ta,zzr7gn_tb,zzr7gn_v1,zzr7gn_v2",
                column("SELECT relname FROM pg_class WHERE relname LIKE 'zzr7gn\\_%' ORDER BY 1"));

        // The partition is still a partition of the table it was declared under, and still takes
        // the rows the partitioned table is written with.
        assertEquals(1, num("SELECT count(*) FROM pg_inherits"
                + " WHERE inhrelid = 'zzr7gn_h1'::regclass"));
        exec("INSERT INTO zzr7gn_hq VALUES (5)");
        assertEquals(1, num("SELECT count(*) FROM zzr7gn_h1"));

        // The indexes came back with their definitions, and the unique one still refuses a
        // duplicate.
        assertEquals("CREATE UNIQUE INDEX zzr7gn_i1 ON public.zzr7gn_ix USING btree (i)",
                indexDef("zzr7gn_i1"));
        assertEquals("CREATE INDEX zzr7gn_i2 ON public.zzr7gn_ix USING btree (j)",
                indexDef("zzr7gn_i2"));
        exec("INSERT INTO zzr7gn_ix VALUES (1,1)");
        assertEquals("23505", stateOf("INSERT INTO zzr7gn_ix VALUES (1,2)"));
        assertEquals(1, num("SELECT count(*) FROM zzr7gn_ix"));

        // The enums came back with their labels, the sequences from where they stood, and the
        // views still read.
        assertEquals("a,b,c", column("SELECT enumlabel FROM pg_enum e"
                + " JOIN pg_type t ON t.oid = e.enumtypid"
                + " WHERE t.typname LIKE 'zzr7gn\\_e%' ORDER BY 1"));
        assertEquals("a", scalar("SELECT 'a'::zzr7gn_e1"));
        assertEquals(1, num("SELECT nextval('zzr7gn_s1')"));
        assertEquals(0, num("SELECT count(*) FROM zzr7gn_v2"));

        exec("DROP TABLE zzr7gn_ta CASCADE");
        exec("DROP TABLE zzr7gn_tb");
        exec("DROP TABLE zzr7gn_hq");
        exec("DROP TABLE zzr7gn_ix");
        exec("DROP SEQUENCE zzr7gn_s1, zzr7gn_s2");
        exec("DROP TYPE zzr7gn_e1, zzr7gn_e2");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * A savepoint holds the whole DROP too: rolling back to one taken before it puts every name
     * it was given back, and leaves what the transaction did before the savepoint standing.
     */
    @Test
    void aDropListRolledBackToASavepointPutsAllOfItsNamesBack() throws Exception {
        exec("CREATE TABLE zzr7gn_ka (i int)");
        exec("CREATE TABLE zzr7gn_kb (i int)");
        exec("CREATE TABLE zzr7gn_kq (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzr7gn_k1 PARTITION OF zzr7gn_kq FOR VALUES FROM (0) TO (10)");

        conn.setAutoCommit(false);
        try {
            exec("DROP TABLE zzr7gn_ka");
            Savepoint sp = conn.setSavepoint("zzr7gn_sp");
            exec("DROP TABLE zzr7gn_kq, zzr7gn_k1");
            assertEquals(1, num("SELECT count(*) FROM pg_class"
                    + " WHERE relname LIKE 'zzr7gn\\_k%'"));
            conn.rollback(sp);
            assertEquals("zzr7gn_k1,zzr7gn_kb,zzr7gn_kq",
                    column("SELECT relname FROM pg_class WHERE relname LIKE 'zzr7gn\\_k%'"
                            + " ORDER BY 1"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }

        // The DROP taken before the savepoint stands; the list rolled back to it does not.
        assertEquals("zzr7gn_k1,zzr7gn_kb,zzr7gn_kq",
                column("SELECT relname FROM pg_class WHERE relname LIKE 'zzr7gn\\_k%' ORDER BY 1"));
        assertEquals(1, num("SELECT count(*) FROM pg_inherits"
                + " WHERE inhrelid = 'zzr7gn_k1'::regclass"));

        exec("DROP TABLE zzr7gn_kb");
        exec("DROP TABLE zzr7gn_kq");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }

    /**
     * Inside a transaction block a DROP whose list holds a name that was never there takes none
     * of the names it was given, and the savepoint the block was rolled back to finds them all.
     */
    @Test
    void aDropListThatFailsInsideATransactionBlockTakesNoneOfItsNames() throws Exception {
        exec("CREATE TABLE zzr7gn_fa (i int)");
        exec("CREATE TABLE zzr7gn_fb (i int)");

        conn.setAutoCommit(false);
        try {
            Savepoint sp = conn.setSavepoint("zzr7gn_fsp");
            assertEquals("42P01",
                    stateOf("DROP TABLE zzr7gn_fa, zzr7gn_fb, zzr7gn_nosuch"));
            conn.rollback(sp);
            assertEquals("zzr7gn_fa,zzr7gn_fb",
                    column("SELECT relname FROM pg_class WHERE relname LIKE 'zzr7gn\\_f%'"
                            + " ORDER BY 1"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }

        assertEquals("zzr7gn_fa,zzr7gn_fb",
                column("SELECT relname FROM pg_class WHERE relname LIKE 'zzr7gn\\_f%' ORDER BY 1"));

        exec("DROP TABLE zzr7gn_fa, zzr7gn_fb");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzr7gn\\_%'"));
    }
}
