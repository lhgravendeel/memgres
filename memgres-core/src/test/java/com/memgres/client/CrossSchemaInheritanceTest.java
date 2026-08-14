package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A parent may be written with the schema that holds it. CREATE TABLE t.c PARTITION OF s.p and
 * CREATE TABLE t.c (...) INHERITS (s.b) did not work at all: the qualified parent name was not
 * read, so nothing could be a partition of, or inherit from, a relation in another schema -- and
 * the check that refuses the same parent twice keyed on the bare name, which is not what
 * PostgreSQL refuses.
 *
 * <p>Every expectation here was measured against PostgreSQL 18 first. What the two engines still
 * answer differently is left out: pg_inherits.inhseqno for a partition, relispartition on a
 * partition's index copy, and the resolution of a bare parent name written inside CREATE TABLE.
 */
class CrossSchemaInheritanceTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void freshSchemas() throws Exception {
        // a test that opened a transaction and failed must not strand the connection in it;
        // ROLLBACK outside a transaction is accepted, as PostgreSQL 18 accepts it
        exec("ROLLBACK");
        exec("SET search_path = public");
        exec("DROP SCHEMA IF EXISTS zzt4b_s CASCADE");
        exec("DROP SCHEMA IF EXISTS zzt4b_t CASCADE");
        exec("DROP SCHEMA IF EXISTS zzt4b_u CASCADE");
        exec("DROP SCHEMA IF EXISTS \"zzt4b_MiX\" CASCADE");
        exec("CREATE SCHEMA zzt4b_s");
        exec("CREATE SCHEMA zzt4b_t");
        exec("CREATE SCHEMA zzt4b_u");
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static String scalar(String sql) throws SQLException {
        List<String> r = rows(sql);
        assertEquals(1, r.size(), "expected one row from: " + sql);
        return r.get(0);
    }

    /** Asserts the statement fails with the SQLSTATE and wording PostgreSQL 18 answers. */
    private static void assertFails(String sqlState, String message, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(message),
                "expected \"" + message + "\" in: " + e.getMessage());
    }

    private static final String INHERITS_OF =
            "SELECT pn.nspname, pc.relname, cn.nspname, cc.relname FROM pg_inherits i"
                    + " JOIN pg_class pc ON pc.oid = i.inhparent"
                    + " JOIN pg_namespace pn ON pn.oid = pc.relnamespace"
                    + " JOIN pg_class cc ON cc.oid = i.inhrelid"
                    + " JOIN pg_namespace cn ON cn.oid = cc.relnamespace"
                    + " WHERE pn.nspname IN ('zzt4b_s','zzt4b_t','zzt4b_u') ORDER BY 1,2,3,4";

    private static final String RELATIONS =
            "SELECT n.nspname, c.relname, c.relkind, c.relispartition FROM pg_class c"
                    + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    + " WHERE n.nspname IN ('zzt4b_s','zzt4b_t','zzt4b_u') AND c.relkind IN ('r','p')"
                    + " ORDER BY 1,2";

    // ---- the creation forms ----

    @Test
    void aPartitionMayBeCreatedInAnotherSchemaThanTheTableItPartitions() throws Exception {
        exec("CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_s.c0 PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzt4b_t.c1 PARTITION OF zzt4b_s.p FOR VALUES FROM (10) TO (20)");
        exec("CREATE TABLE zzt4b_t.cd PARTITION OF zzt4b_s.p DEFAULT");
        exec("INSERT INTO zzt4b_s.p VALUES (1,'a'), (11,'b'), (99,'z')");

        // each row reached the partition its key belongs to, whichever schema holds it
        assertEquals(List.of("1|a"), rows("SELECT i, k FROM zzt4b_s.c0 ORDER BY i"));
        assertEquals(List.of("11|b"), rows("SELECT i, k FROM zzt4b_t.c1 ORDER BY i"));
        assertEquals(List.of("99|z"), rows("SELECT i, k FROM zzt4b_t.cd ORDER BY i"),
                "the row no bound accepts belongs to the DEFAULT partition in the other schema");
        assertEquals("3", scalar("SELECT count(*)::int FROM zzt4b_s.p"));

        assertEquals(List.of(
                        "zzt4b_s|c0|r|t",
                        "zzt4b_s|p|p|f",
                        "zzt4b_t|c1|r|t",
                        "zzt4b_t|cd|r|t"),
                rows(RELATIONS),
                "the partitioned table is not itself a partition; each partition is one");

        assertEquals(List.of(
                        "zzt4b_s|p|zzt4b_s|c0",
                        "zzt4b_s|p|zzt4b_t|c1",
                        "zzt4b_s|p|zzt4b_t|cd"),
                rows(INHERITS_OF),
                "pg_inherits joins the parent's namespace to the child's");

        assertEquals("FOR VALUES FROM (10) TO (20)",
                scalar("SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = 'zzt4b_t' AND c.relname = 'c1'"));
    }

    @Test
    void anUnqualifiedPartitionOfAQualifiedParentIsCreatedWhereTheSearchPathSays() throws Exception {
        exec("SET search_path = zzt4b_t");
        exec("CREATE TABLE zzt4b_s.hash (i int, k text) PARTITION BY HASH (i)");
        exec("CREATE TABLE h0 PARTITION OF zzt4b_s.hash FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        exec("CREATE TABLE h1 PARTITION OF zzt4b_s.hash FOR VALUES WITH (MODULUS 2, REMAINDER 1)");

        assertEquals(List.of("zzt4b_t|h0|t", "zzt4b_t|h1|t"),
                rows("SELECT n.nspname, c.relname, c.relispartition FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE c.relname IN ('h0','h1') ORDER BY 1,2"),
                "the partition goes where the search path says, not where its parent lives");

        exec("INSERT INTO zzt4b_s.hash SELECT g, 'v' FROM generate_series(1,20) g");
        assertEquals("20", scalar("SELECT count(*)::int FROM zzt4b_s.hash"));
        assertEquals("t", scalar("SELECT (SELECT count(*) FROM zzt4b_t.h0)"
                        + " + (SELECT count(*) FROM zzt4b_t.h1) = 20"),
                "every row reached one of the two partitions in the other schema");
    }

    @Test
    void aQualifiedParentIsResolvedThroughTheSchemaWrittenNotTheSearchPath() throws Exception {
        exec("CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_t.p (i int, k text) PARTITION BY RANGE (i)");
        exec("SET search_path = zzt4b_t, public");
        exec("CREATE TABLE zzt4b_u.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");

        assertEquals(List.of("zzt4b_s|p|zzt4b_u|c"), rows(INHERITS_OF),
                "the partition belongs to the table the qualifier named, not the one the path finds");

        exec("INSERT INTO zzt4b_s.p VALUES (1,'s')");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_u.c"));

        assertFails("23514", "no partition of relation \"p\" found for row",
                "INSERT INTO zzt4b_t.p VALUES (1,'t')");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_u.c"),
                "the table the search path would have found was left with no partition");
    }

    @Test
    void aQualifiedInheritsParentIsResolvedThroughTheSchemaWritten() throws Exception {
        exec("CREATE TABLE zzt4b_s.b (i int, s_col text)");
        exec("CREATE TABLE zzt4b_t.b (i int, t_col text)");
        exec("SET search_path = zzt4b_t, public");
        exec("CREATE TABLE zzt4b_u.kid (j int) INHERITS (zzt4b_s.b)");

        assertEquals(List.of("i", "s_col", "j"),
                rows("SELECT column_name FROM information_schema.columns"
                        + " WHERE table_schema = 'zzt4b_u' AND table_name = 'kid'"
                        + " ORDER BY ordinal_position"),
                "the columns are the ones the named schema's table declares");
        assertEquals(List.of("zzt4b_s|b|zzt4b_u|kid"), rows(INHERITS_OF));
    }

    // ---- ATTACH and DETACH ----

    @Test
    void attachAndDetachNameBothRelationsWithTheirSchemas() throws Exception {
        exec("CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_s.c0 PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzt4b_t.free (i int, k text)");
        exec("ALTER TABLE zzt4b_s.p ATTACH PARTITION zzt4b_t.free FOR VALUES FROM (20) TO (30)");

        assertEquals("t", scalar("SELECT c.relispartition FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'zzt4b_t' AND c.relname = 'free'"));

        exec("INSERT INTO zzt4b_s.p VALUES (25,'y')");
        assertEquals(List.of("25|y"), rows("SELECT i, k FROM zzt4b_t.free ORDER BY i"),
                "the attached table in the other schema takes the rows its bound accepts");

        exec("ALTER TABLE zzt4b_s.p DETACH PARTITION zzt4b_t.free");

        assertEquals("f", scalar("SELECT c.relispartition FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = 'zzt4b_t' AND c.relname = 'free'"),
                "after DETACH it is a table of its own again");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_t.free"),
                "keeping the rows it was given");
        assertEquals("0", scalar("SELECT count(*)::int FROM zzt4b_s.p"),
                "which the parent no longer reads");
        assertEquals(List.of("zzt4b_s|p|zzt4b_s|c0"), rows(INHERITS_OF),
                "and the inheritance row went with the attachment");
    }

    @Test
    void attachAndDetachKeepAQuotedMixedCaseNameWhole() throws Exception {
        exec("CREATE SCHEMA \"zzt4b_MiX\"");
        exec("CREATE TABLE zzt4b_s.pt (i int, k text) PARTITION BY LIST (i)");
        exec("CREATE TABLE \"zzt4b_MiX\".\"Part1\" PARTITION OF zzt4b_s.pt FOR VALUES IN (1)");
        exec("INSERT INTO zzt4b_s.pt VALUES (1,'one')");

        assertEquals(List.of("1|one"), rows("SELECT i, k FROM \"zzt4b_MiX\".\"Part1\" ORDER BY 1"));
        assertEquals("t", scalar("SELECT c.relispartition FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'zzt4b_MiX' AND c.relname = 'Part1'"));

        exec("ALTER TABLE zzt4b_s.pt DETACH PARTITION \"zzt4b_MiX\".\"Part1\"");
        assertEquals("f", scalar("SELECT c.relispartition FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'zzt4b_MiX' AND c.relname = 'Part1'"));
        assertEquals("0", scalar("SELECT count(*)::int FROM zzt4b_s.pt"));

        exec("ALTER TABLE zzt4b_s.pt ATTACH PARTITION \"zzt4b_MiX\".\"Part1\" FOR VALUES IN (1)");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_s.pt"),
                "the rows come back with the partition that was attached again");
        assertEquals("t", scalar("SELECT c.relispartition FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'zzt4b_MiX' AND c.relname = 'Part1'"));
    }

    @Test
    void attachAttachesTheRelationTheQualifierNamed() throws Exception {
        exec("CREATE TABLE zzt4b_s.q (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_t.part (i int, k text)");
        exec("CREATE TABLE zzt4b_u.part (i int, k text)");
        exec("ALTER TABLE zzt4b_s.q ATTACH PARTITION zzt4b_u.part FOR VALUES FROM (0) TO (10)");

        assertEquals(List.of("zzt4b_t|part|f", "zzt4b_u|part|t"),
                rows("SELECT n.nspname, c.relname, c.relispartition FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE c.relname = 'part' ORDER BY 1"),
                "only the relation the qualifier named became a partition");

        exec("INSERT INTO zzt4b_s.q VALUES (5,'five')");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_u.part"));
        assertEquals("0", scalar("SELECT count(*)::int FROM zzt4b_t.part"));
    }

    @Test
    void detachIsRefusedForARelationThatIsNotThisParentsPartition() throws Exception {
        exec("CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_t.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_u.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzt4b_u.other (i int, k text)");

        assertFails("42P01", "relation \"other\" is not a partition of relation \"p\"",
                "ALTER TABLE zzt4b_s.p DETACH PARTITION zzt4b_u.other");
        assertFails("42P01", "relation \"c\" is not a partition of relation \"p\"",
                "ALTER TABLE zzt4b_t.p DETACH PARTITION zzt4b_u.c");

        assertEquals(List.of("zzt4b_s|p|zzt4b_u|c"), rows(INHERITS_OF),
                "the partition of the table that was not named is untouched");
    }

    // ---- a hierarchy spread over three schemas ----

    @Test
    void aThreeSchemaHierarchyRoutesReadsWritesAndDdlThroughEveryLevel() throws Exception {
        exec("CREATE TABLE zzt4b_s.h (i int NOT NULL, k text CHECK (k <> 'no')) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_t.mid PARTITION OF zzt4b_s.h FOR VALUES FROM (0) TO (100)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_u.leaf PARTITION OF zzt4b_t.mid FOR VALUES FROM (0) TO (50)");
        exec("CREATE TABLE zzt4b_u.leaf2 PARTITION OF zzt4b_t.mid FOR VALUES FROM (50) TO (100)");
        exec("INSERT INTO zzt4b_s.h VALUES (1,'a'), (60,'b')");

        assertEquals(List.of("1|a"), rows("SELECT i, k FROM zzt4b_u.leaf ORDER BY i"),
                "a row written at the root is routed two levels down, across two schemas");
        assertEquals(List.of("1|a", "60|b"), rows("SELECT i, k FROM zzt4b_t.mid ORDER BY i"));

        assertFails("23514", "new row for relation \"leaf\" violates check constraint \"h_k_check\"",
                "INSERT INTO zzt4b_s.h VALUES (2,'no')");
        assertFails("23514", "new row for relation \"leaf\" violates partition constraint",
                "INSERT INTO zzt4b_u.leaf VALUES (60,'c')");
        assertFails("23514", "no partition of relation \"h\" found for row",
                "INSERT INTO zzt4b_s.h VALUES (500,'far')");

        exec("UPDATE zzt4b_s.h SET i = 70 WHERE i = 1");
        assertEquals(List.of("60|b", "70|a"), rows("SELECT i, k FROM zzt4b_u.leaf2 ORDER BY i"),
                "an update that moves the key moves the row to the leaf that now holds it");
        assertEquals("0", scalar("SELECT count(*)::int FROM zzt4b_u.leaf"));

        exec("ALTER TABLE zzt4b_s.h ADD COLUMN extra int DEFAULT 7");
        assertEquals(List.of("i", "k", "extra"),
                rows("SELECT column_name FROM information_schema.columns"
                        + " WHERE table_schema = 'zzt4b_u' AND table_name = 'leaf2'"
                        + " ORDER BY ordinal_position"),
                "a column added at the root reaches the leaves in the third schema");

        assertEquals(List.of(
                        "zzt4b_s|h|p|f",
                        "zzt4b_t|mid|p|t",
                        "zzt4b_u|leaf|r|t",
                        "zzt4b_u|leaf2|r|t"),
                rows(RELATIONS),
                "the middle level is a partition of the root and a partitioned table at once");
        assertEquals(List.of(
                        "zzt4b_s|h|zzt4b_t|mid",
                        "zzt4b_t|mid|zzt4b_u|leaf",
                        "zzt4b_t|mid|zzt4b_u|leaf2"),
                rows(INHERITS_OF));

        exec("TRUNCATE zzt4b_s.h");
        assertEquals("0", scalar("SELECT count(*)::int FROM zzt4b_u.leaf2"),
                "TRUNCATE at the root empties the leaves in the other schemas");

        exec("DROP TABLE zzt4b_s.h");
        assertEquals(List.of(), rows(RELATIONS),
                "and dropping the root takes every level of the hierarchy with it");
    }

    // ---- INHERITS with qualified parents ----

    @Test
    void inheritsMayNameSeveralQualifiedParentsAndMergesThemInOrder() throws Exception {
        exec("CREATE TABLE zzt4b_s.a1 (sa int)");
        exec("CREATE TABLE zzt4b_t.a2 (ta text)");
        exec("CREATE TABLE zzt4b_u.a3 (ua date)");
        exec("CREATE TABLE zzt4b_s.three (own int) INHERITS (zzt4b_s.a1, zzt4b_t.a2, zzt4b_u.a3)");

        assertEquals(List.of("sa|1", "ta|2", "ua|3", "own|4"),
                rows("SELECT column_name, ordinal_position FROM information_schema.columns"
                        + " WHERE table_schema = 'zzt4b_s' AND table_name = 'three'"
                        + " ORDER BY ordinal_position"),
                "the inherited columns come first, in the order the parents were named");

        assertEquals(List.of("zzt4b_s|a1|1", "zzt4b_t|a2|2", "zzt4b_u|a3|3"),
                rows("SELECT pn.nspname, pc.relname, i.inhseqno FROM pg_inherits i"
                        + " JOIN pg_class pc ON pc.oid = i.inhparent"
                        + " JOIN pg_namespace pn ON pn.oid = pc.relnamespace"
                        + " JOIN pg_class cc ON cc.oid = i.inhrelid"
                        + " JOIN pg_namespace cn ON cn.oid = cc.relnamespace"
                        + " WHERE cn.nspname = 'zzt4b_s' AND cc.relname = 'three' ORDER BY i.inhseqno"),
                "pg_inherits numbers a child's parents in the order they were written");

        assertEquals("t", scalar("SELECT c.relhassubclass FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = 'zzt4b_t' AND c.relname = 'a2'"),
                "a parent in another schema is marked as having a child");
    }

    @Test
    void aWrittenSchemaAndANameLeftToTheSearchPathMayStandSideBySide() throws Exception {
        exec("CREATE TABLE zzt4b_s.a1 (sa int)");
        exec("CREATE TABLE zzt4b_t.a2 (ta text)");
        exec("SET search_path = zzt4b_t");
        exec("CREATE TABLE zzt4b_u.mixed (m int) INHERITS (zzt4b_s.a1, a2)");

        assertEquals(List.of("sa|1", "ta|2", "m|3"),
                rows("SELECT column_name, ordinal_position FROM information_schema.columns"
                        + " WHERE table_schema = 'zzt4b_u' AND table_name = 'mixed'"
                        + " ORDER BY ordinal_position"));
        assertEquals(List.of("zzt4b_s|a1|zzt4b_u|mixed", "zzt4b_t|a2|zzt4b_u|mixed"),
                rows(INHERITS_OF));
    }

    @Test
    void twoParentsOfTheSameNameInTwoSchemasAreTwoParents() throws Exception {
        exec("CREATE TABLE zzt4b_s.b (i int, a text)");
        exec("CREATE TABLE zzt4b_t.b (i int, x text)");
        exec("CREATE TABLE zzt4b_s.two (j int) INHERITS (zzt4b_s.b, zzt4b_t.b)");

        assertEquals(List.of("i|1", "a|2", "x|3", "j|4"),
                rows("SELECT column_name, ordinal_position FROM information_schema.columns"
                        + " WHERE table_schema = 'zzt4b_s' AND table_name = 'two'"
                        + " ORDER BY ordinal_position"),
                "the column both parents declare is merged into one");

        assertEquals(List.of("zzt4b_s|b|1", "zzt4b_t|b|2"),
                rows("SELECT pn.nspname, pc.relname, i.inhseqno FROM pg_inherits i"
                        + " JOIN pg_class pc ON pc.oid = i.inhparent"
                        + " JOIN pg_namespace pn ON pn.oid = pc.relnamespace"
                        + " JOIN pg_class cc ON cc.oid = i.inhrelid"
                        + " JOIN pg_namespace cn ON cn.oid = cc.relnamespace"
                        + " WHERE cn.nspname = 'zzt4b_s' AND cc.relname = 'two' ORDER BY i.inhseqno"));

        assertEquals("f|r", scalar("SELECT c.relispartition, c.relkind FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = 'zzt4b_s' AND c.relname = 'two'"),
                "a child of two ordinary tables is not a partition of either");

        exec("INSERT INTO zzt4b_s.two VALUES (1,'a','x',9)");
        assertEquals(List.of("1|a"), rows("SELECT i, a FROM zzt4b_s.b ORDER BY 1"),
                "the row is read through the parent, which holds none of its own");
        assertEquals("0", scalar("SELECT count(*)::int FROM ONLY zzt4b_s.b"));
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_t.b"),
                "and through the second parent, in the other schema, just as well");
        assertEquals("0", scalar("SELECT count(*)::int FROM ONLY zzt4b_t.b"));

        // the same relation twice is what PostgreSQL refuses, and it names it without its schema
        assertFails("42P07", "relation \"b\" would be inherited from more than once",
                "CREATE TABLE zzt4b_s.dup (j int) INHERITS (zzt4b_s.b, zzt4b_s.b)");
    }

    @Test
    void alterOnAQualifiedParentReachesTheChildAndTheChildsChild() throws Exception {
        exec("CREATE TABLE zzt4b_s.b (i int, a text)");
        exec("CREATE TABLE zzt4b_t.b (i int, x text)");
        exec("CREATE TABLE zzt4b_s.two (j int) INHERITS (zzt4b_s.b, zzt4b_t.b)");
        exec("ALTER TABLE zzt4b_s.b ADD COLUMN later int");

        assertEquals(List.of("i|1", "a|2", "x|3", "j|4", "later|5"),
                rows("SELECT column_name, ordinal_position FROM information_schema.columns"
                        + " WHERE table_schema = 'zzt4b_s' AND table_name = 'two'"
                        + " ORDER BY ordinal_position"));

        exec("CREATE TABLE zzt4b_t.chain (z int) INHERITS (zzt4b_s.two)");
        assertEquals(List.of("i", "a", "x", "j", "later", "z"),
                rows("SELECT column_name FROM information_schema.columns"
                        + " WHERE table_schema = 'zzt4b_t' AND table_name = 'chain'"
                        + " ORDER BY ordinal_position"),
                "a child of that child, in a third schema, holds every column of both");
    }

    @Test
    void aQuotedMixedCaseParentHandsDownItsDefaultNotNullAndCheck() throws Exception {
        exec("CREATE SCHEMA \"zzt4b_MiX\"");
        exec("CREATE TABLE \"zzt4b_MiX\".\"Par\" (i int NOT NULL DEFAULT 3 CHECK (i > 0), t text)");
        exec("CREATE TABLE zzt4b_s.kid () INHERITS (\"zzt4b_MiX\".\"Par\")");
        exec("INSERT INTO zzt4b_s.kid (t) VALUES ('x')");

        assertEquals(List.of("3|x"), rows("SELECT i, t FROM zzt4b_s.kid ORDER BY 1"),
                "the parent's DEFAULT was used for the column the insert did not name");
        assertEquals(List.of("3|x"), rows("SELECT i, t FROM \"zzt4b_MiX\".\"Par\" ORDER BY 1"));
        assertEquals("0", scalar("SELECT count(*)::int FROM ONLY \"zzt4b_MiX\".\"Par\""));

        assertFails("23514", "new row for relation \"kid\" violates check constraint \"Par_i_check\"",
                "INSERT INTO zzt4b_s.kid (i, t) VALUES (0, 'bad')");
        assertFails("23502", "null value in column \"i\" of relation \"kid\" violates not-null constraint",
                "INSERT INTO zzt4b_s.kid (i, t) VALUES (NULL, 'bad')");

        assertEquals(List.of("Par_i_check", "Par_i_not_null"),
                rows("SELECT conname FROM pg_constraint co"
                        + " JOIN pg_class c ON c.oid = co.conrelid"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = 'zzt4b_s' AND c.relname = 'kid' ORDER BY 1"),
                "both stand on the child, named for the parent that handed them down");

        assertEquals(List.of("zzt4b_MiX|Par|1"),
                rows("SELECT pn.nspname, pc.relname, i.inhseqno FROM pg_inherits i"
                        + " JOIN pg_class pc ON pc.oid = i.inhparent"
                        + " JOIN pg_namespace pn ON pn.oid = pc.relnamespace"
                        + " JOIN pg_class cc ON cc.oid = i.inhrelid"
                        + " JOIN pg_namespace cn ON cn.oid = cc.relnamespace"
                        + " WHERE cn.nspname = 'zzt4b_s' AND cc.relname = 'kid' ORDER BY i.inhseqno"));
    }

    // ---- what a qualified name is refused for ----

    @Test
    void aQualifierNamingNoSchemaOrNoRelationIsRefusedAndLeavesNothingBehind() throws Exception {
        exec("CREATE TABLE zzt4b_s.q (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_s.plain (i int)");
        exec("CREATE TABLE zzt4b_u.part (i int, k text)");
        exec("ALTER TABLE zzt4b_s.q ATTACH PARTITION zzt4b_u.part FOR VALUES FROM (0) TO (10)");

        // PARTITION OF
        assertFails("42P17", "\"plain\" is not partitioned",
                "CREATE TABLE zzt4b_t.notpart PARTITION OF zzt4b_s.plain FOR VALUES FROM (0) TO (10)");
        assertFails("3F000", "schema \"zzt4b_nosuch\" does not exist",
                "CREATE TABLE zzt4b_t.nsp PARTITION OF zzt4b_nosuch.q FOR VALUES FROM (10) TO (20)");
        assertFails("42P01", "relation \"zzt4b_s.nosuch\" does not exist",
                "CREATE TABLE zzt4b_t.nrel PARTITION OF zzt4b_s.nosuch FOR VALUES FROM (10) TO (20)");
        assertFails("3F000", "schema \"zzt4b_nosuch\" does not exist",
                "CREATE TABLE zzt4b_nosuch.c PARTITION OF zzt4b_s.q FOR VALUES FROM (10) TO (20)");
        assertFails("42P17", "partition \"over\" would overlap partition \"part\"",
                "CREATE TABLE zzt4b_t.over PARTITION OF zzt4b_s.q FOR VALUES FROM (5) TO (15)");

        // INHERITS
        assertFails("3F000", "schema \"zzt4b_nosuch\" does not exist",
                "CREATE TABLE zzt4b_s.noschema (j int) INHERITS (zzt4b_nosuch.b)");
        assertFails("42P01", "relation \"zzt4b_s.nosuch\" does not exist",
                "CREATE TABLE zzt4b_s.norel (j int) INHERITS (zzt4b_s.nosuch)");

        // ATTACH
        assertFails("3F000", "schema \"zzt4b_nosuch\" does not exist",
                "ALTER TABLE zzt4b_s.q ATTACH PARTITION zzt4b_nosuch.part FOR VALUES FROM (10) TO (20)");
        assertFails("3F000", "schema \"zzt4b_nosuch\" does not exist",
                "ALTER TABLE zzt4b_nosuch.q ATTACH PARTITION zzt4b_u.part FOR VALUES FROM (10) TO (20)");
        assertFails("42809", "\"part\" is already a partition",
                "ALTER TABLE zzt4b_s.q ATTACH PARTITION zzt4b_u.part FOR VALUES FROM (10) TO (20)");

        assertEquals(List.of("zzt4b_s|plain|r|f", "zzt4b_s|q|p|f", "zzt4b_u|part|r|t"),
                rows(RELATIONS),
                "no refusal created a relation and none changed what was already there");
    }

    @Test
    void aPartitionNameTheTargetSchemaAlreadyHoldsIsRefused() throws Exception {
        exec("CREATE TABLE zzt4b_s.q (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_t.part (i int, k text)");

        assertFails("42P07", "relation \"part\" already exists",
                "CREATE TABLE zzt4b_t.part PARTITION OF zzt4b_s.q FOR VALUES FROM (10) TO (20)");
        assertEquals(List.of("zzt4b_s|q|p|f", "zzt4b_t|part|r|f"), rows(RELATIONS),
                "the relation that held the name is not a partition of anything");
    }

    // ---- the index copies that follow a partition ----

    @Test
    void anIndexCopyLandsInThePartitionsOwnSchema() throws Exception {
        exec("CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX ON zzt4b_s.p (k)");
        exec("CREATE TABLE zzt4b_t.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");

        assertEquals(List.of("zzt4b_s|p|p_k_idx", "zzt4b_t|c|c_k_idx"),
                rows("SELECT schemaname, tablename, indexname FROM pg_indexes"
                        + " WHERE schemaname IN ('zzt4b_s','zzt4b_t') ORDER BY 1,2,3"),
                "an index declared before the partition existed is copied into the partition's schema");

        exec("CREATE UNIQUE INDEX ON zzt4b_s.p (i)");
        assertEquals(List.of("zzt4b_s|p|p_i_idx", "zzt4b_s|p|p_k_idx",
                        "zzt4b_t|c|c_i_idx", "zzt4b_t|c|c_k_idx"),
                rows("SELECT schemaname, tablename, indexname FROM pg_indexes"
                        + " WHERE schemaname IN ('zzt4b_s','zzt4b_t') ORDER BY 1,2,3"),
                "and so is one declared after it");

        assertEquals(List.of("p_i_idx|zzt4b_t|c_i_idx", "p_k_idx|zzt4b_t|c_k_idx"),
                rows("SELECT pc.relname, cn.nspname, cc.relname FROM pg_inherits i"
                        + " JOIN pg_class pc ON pc.oid = i.inhparent"
                        + " JOIN pg_class cc ON cc.oid = i.inhrelid"
                        + " JOIN pg_namespace cn ON cn.oid = cc.relnamespace"
                        + " WHERE cc.relkind = 'i' AND cn.nspname IN ('zzt4b_s','zzt4b_t')"
                        + " ORDER BY 1,2,3"),
                "each copy is recorded as a child of the index it was copied from");

        exec("INSERT INTO zzt4b_s.p VALUES (1,'x')");
        assertFails("23505", "duplicate key value violates unique constraint \"c_i_idx\"",
                "INSERT INTO zzt4b_s.p VALUES (1,'y')");
        assertFails("23505", "duplicate key value violates unique constraint \"c_i_idx\"",
                "INSERT INTO zzt4b_t.c VALUES (1,'z')");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_s.p"));

        exec("CREATE TABLE zzt4b_u.mid PARTITION OF zzt4b_s.p FOR VALUES FROM (10) TO (100)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_t.leaf PARTITION OF zzt4b_u.mid FOR VALUES FROM (10) TO (20)");
        assertEquals(List.of(
                        "zzt4b_s|p|p_i_idx", "zzt4b_s|p|p_k_idx",
                        "zzt4b_t|c|c_i_idx", "zzt4b_t|c|c_k_idx",
                        "zzt4b_t|leaf|leaf_i_idx", "zzt4b_t|leaf|leaf_k_idx",
                        "zzt4b_u|mid|mid_i_idx", "zzt4b_u|mid|mid_k_idx"),
                rows("SELECT schemaname, tablename, indexname FROM pg_indexes"
                        + " WHERE schemaname IN ('zzt4b_s','zzt4b_t','zzt4b_u') ORDER BY 1,2,3"),
                "every level of a three-schema hierarchy gets a copy in its own schema");
    }

    // ---- what a transaction undoes (a corpus file cannot hold a transaction) ----

    @Test
    void aCrossSchemaPartitionAndChildCreatedInATransactionAreUndoneByRollback() throws Exception {
        exec("CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_s.b (i int)");

        exec("BEGIN");
        exec("CREATE TABLE zzt4b_t.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzt4b_t.kid (j int) INHERITS (zzt4b_s.b)");
        exec("INSERT INTO zzt4b_s.p VALUES (1,'x')");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_t.c"),
                "inside the transaction the row reached the partition in the other schema");
        exec("ROLLBACK");

        assertEquals("0", scalar("SELECT count(*)::int FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'zzt4b_t'"),
                "neither relation survived the rollback");
        assertEquals("0", scalar("SELECT count(*)::int FROM zzt4b_s.p"));
        assertEquals(List.of(), rows(INHERITS_OF),
                "and no inheritance row was left pointing at a relation that is gone");

        exec("BEGIN");
        exec("CREATE TABLE zzt4b_t.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");
        exec("COMMIT");
        exec("INSERT INTO zzt4b_s.p VALUES (1,'x')");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_t.c"),
                "committed, the same statement leaves a partition that routes rows");
    }

    @Test
    void aCrossSchemaPartitionCreatedAfterASavepointIsUndoneByRollbackToIt() throws Exception {
        exec("CREATE TABLE zzt4b_s.p (i int, k text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4b_t.c PARTITION OF zzt4b_s.p FOR VALUES FROM (0) TO (10)");
        exec("INSERT INTO zzt4b_s.p VALUES (1,'x')");

        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("CREATE TABLE zzt4b_t.c2 PARTITION OF zzt4b_s.p FOR VALUES FROM (10) TO (20)");
        exec("INSERT INTO zzt4b_s.p VALUES (11,'y')");
        assertEquals("2", scalar("SELECT count(*)::int FROM zzt4b_s.p"));
        exec("ROLLBACK TO SAVEPOINT sp1");
        assertEquals("1", scalar("SELECT count(*)::int FROM zzt4b_s.p"),
                "the row that only the undone partition could hold is gone with it");
        exec("INSERT INTO zzt4b_s.p VALUES (2,'z')");
        exec("COMMIT");

        assertEquals("2", scalar("SELECT count(*)::int FROM zzt4b_s.p"),
                "what the transaction did after the savepoint it rolled back to is kept");
        assertEquals("0", scalar("SELECT count(*)::int FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'zzt4b_t' AND c.relname = 'c2'"));
        assertEquals(List.of("zzt4b_s|p|zzt4b_t|c"), rows(INHERITS_OF));
        assertFails("23514", "no partition of relation \"p\" found for row",
                "INSERT INTO zzt4b_s.p VALUES (11,'y')");
    }
}
