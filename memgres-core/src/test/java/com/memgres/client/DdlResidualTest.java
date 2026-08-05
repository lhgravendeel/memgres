package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The residual checks PostgreSQL 18 makes while a definition is written, and the PG 18 spellings a
 * migration may already use.
 *
 * <p>A retype rewrites every stored value, so what was declared over the column has to still hold
 * over the new ones; a TRUNCATE is blocked by a foreign key's existence rather than by the rows
 * behind it; a rename onto a taken name must not destroy what holds it; and VACUUM ONLY, ANALYZE
 * ONLY and UNIQUE ... WITHOUT OVERLAPS are statements a PG 18 dump can contain.
 *
 * <p>Every refusal here is paired with the ordinary shapes around it. A rule that fires on a
 * definition PostgreSQL accepts breaks a migration, which costs more than the permissiveness it
 * removes, so the accepted shapes are pinned as firmly as the refused ones.
 */
class DdlResidualTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    private static List<String> labels(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData m = rs.getMetaData();
            for (int i = 1; i <= m.getColumnCount(); i++) out.add(m.getColumnLabel(i));
        }
        return out;
    }

    private static SQLException assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
        return e;
    }

    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    // ---------------------------------------------------------------- SECTION A
    // A retype has to satisfy what is already declared over the column.

    @Test
    void aRetypeThatCollapsesTwoKeysOntoOneIsRefused() throws Exception {
        exec("CREATE TABLE dr_pk (a numeric PRIMARY KEY)");
        exec("INSERT INTO dr_pk VALUES (1.0),(1.4)");
        SQLException e = assertRejected("23505", "could not create unique index",
                "ALTER TABLE dr_pk ALTER COLUMN a TYPE int");
        // PostgreSQL sends the duplicated key as its own Detail field, not inside the message.
        ServerErrorMessage sem = ((PSQLException) e).getServerErrorMessage();
        assertNotNull(sem);
        assertEquals("Key (a)=(1) is duplicated.", sem.getDetail());
        // The refused ALTER left the column and its rows exactly as they were.
        assertEquals(List.of("1.0", "1.4"), column("SELECT a FROM dr_pk ORDER BY 1"));
    }

    @Test
    void aUniqueConstraintBlocksTheSameCollision() throws Exception {
        exec("CREATE TABLE dr_uq (a numeric UNIQUE)");
        exec("INSERT INTO dr_uq VALUES (1.0),(1.4)");
        assertRejected("23505", "could not create unique index",
                "ALTER TABLE dr_uq ALTER COLUMN a TYPE int");
        assertEquals(List.of("1.0", "1.4"), column("SELECT a FROM dr_uq ORDER BY 1"));
    }

    @Test
    void aRetypeThatKeepsTheKeysDistinctIsAccepted() throws Exception {
        exec("CREATE TABLE dr_pk_ok (a numeric PRIMARY KEY)");
        exec("INSERT INTO dr_pk_ok VALUES (1.0),(2.4)");
        assertAccepted("ALTER TABLE dr_pk_ok ALTER COLUMN a TYPE int");
        assertEquals(List.of("1", "2"), column("SELECT a FROM dr_pk_ok ORDER BY 1"));
    }

    @Test
    void aCheckConstraintIsReRunOverTheRewrittenValues() throws Exception {
        exec("CREATE TABLE dr_ck (a numeric, CONSTRAINT dr_ckc CHECK (a < 0.9))");
        exec("INSERT INTO dr_ck VALUES (0.6)");
        assertRejected("23514",
                "check constraint \"dr_ckc\" of relation \"dr_ck\" is violated by some row",
                "ALTER TABLE dr_ck ALTER COLUMN a TYPE int");
        // The table must not be left holding a row its own live constraint rejects.
        assertEquals(List.of("0.6"), column("SELECT a FROM dr_ck ORDER BY 1"));
        assertRejected("23514", "violates check constraint", "INSERT INTO dr_ck VALUES (5)");
    }

    @Test
    void aCheckTheNewValuesStillSatisfyIsNoObstacle() throws Exception {
        exec("CREATE TABLE dr_ck_ok (a numeric CHECK (a >= 0))");
        exec("INSERT INTO dr_ck_ok VALUES (1.2),(3.7)");
        assertAccepted("ALTER TABLE dr_ck_ok ALTER COLUMN a TYPE int");
        assertEquals(List.of("1", "4"), column("SELECT a FROM dr_ck_ok ORDER BY 1"));
    }

    @Test
    void aConstraintAddedNotValidIsNotRevalidatedByARetype() throws Exception {
        exec("CREATE TABLE dr_nv (a numeric)");
        exec("INSERT INTO dr_nv VALUES (0.6)");
        exec("ALTER TABLE dr_nv ADD CONSTRAINT dr_nvc CHECK (a < 0.9) NOT VALID");
        assertAccepted("ALTER TABLE dr_nv ALTER COLUMN a TYPE int");
    }

    @Test
    void aDefaultThatCannotBeCastRefusesTheWholeAlter() throws Exception {
        exec("CREATE TABLE dr_df (a text DEFAULT 'abc')");
        assertRejected("42804", "default for column \"a\" cannot be cast automatically to type integer",
                "ALTER TABLE dr_df ALTER COLUMN a TYPE int USING 0");
        // Having refused, the column still advertises the default it always had.
        assertEquals("'abc'::text", scalar(
                "SELECT column_default FROM information_schema.columns"
                        + " WHERE table_name='dr_df' AND column_name='a'"));
        // A USING clause does not excuse the default.
        exec("CREATE TABLE dr_df2 (a text DEFAULT '7')");
        assertRejected("42804", "default for column \"a\" cannot be cast automatically to type integer",
                "ALTER TABLE dr_df2 ALTER COLUMN a TYPE int USING a::int");
    }

    @Test
    void defaultsThatDoCastAreUntouched() throws Exception {
        exec("CREATE TABLE dr_df_ok (a int DEFAULT 3, b text DEFAULT 'x', c numeric DEFAULT 1.5,"
                + " d timestamp DEFAULT now())");
        assertAccepted("ALTER TABLE dr_df_ok ALTER COLUMN a TYPE bigint");
        assertAccepted("ALTER TABLE dr_df_ok ALTER COLUMN b TYPE varchar(5)");
        assertAccepted("ALTER TABLE dr_df_ok ALTER COLUMN c TYPE int");
        assertAccepted("ALTER TABLE dr_df_ok ALTER COLUMN d TYPE date");
        // A serial keeps its nextval default across a widening, and a column with no default at
        // all retypes under an explicit USING.
        exec("CREATE TABLE dr_ser (id serial PRIMARY KEY, v int)");
        assertAccepted("ALTER TABLE dr_ser ALTER COLUMN id TYPE bigint");
        exec("CREATE TABLE dr_nodf (a text)");
        assertAccepted("ALTER TABLE dr_nodf ALTER COLUMN a TYPE int USING 0");
    }

    // ---------------------------------------------------------------- SECTION B
    // VACUUM ONLY and ANALYZE ONLY.

    @Test
    void vacuumAndAnalyzeReadOnlyBeforeTheRelation() throws Exception {
        exec("CREATE TABLE dr_va (id int PRIMARY KEY, v int)");
        assertAccepted("VACUUM ONLY dr_va");
        assertAccepted("ANALYZE ONLY dr_va");
        assertAccepted("VACUUM ANALYZE ONLY dr_va");
        assertAccepted("VACUUM VERBOSE ONLY dr_va");
        assertAccepted("ANALYZE ONLY dr_va (v)");
        assertAccepted("VACUUM ANALYZE ONLY dr_va (v)");
        // Without ONLY they behave as they always did.
        assertAccepted("VACUUM dr_va");
        assertAccepted("ANALYZE dr_va");
        assertAccepted("VACUUM");
        assertAccepted("ANALYZE");
    }

    @Test
    void onlyDoesNotHideARelationThatIsNotThere() {
        assertRejected("42P01", "relation \"dr_nosuch\" does not exist", "VACUUM ONLY dr_nosuch");
        assertRejected("42P01", "relation \"dr_nosuch\" does not exist", "ANALYZE ONLY dr_nosuch");
        // ONLY is reserved, so it can never be the relation it was meant to qualify.
        assertRejected("42601", "syntax error at end of input", "VACUUM ONLY");
        assertRejected("42601", "syntax error at end of input", "ANALYZE ONLY");
    }

    @Test
    void aQuotedOnlyIsStillAnOrdinaryName() throws Exception {
        exec("CREATE TABLE \"only\" (i int PRIMARY KEY)");
        assertAccepted("VACUUM \"only\"");
        assertAccepted("ANALYZE \"only\"");
        assertAccepted("VACUUM ONLY \"only\"");
        exec("DROP TABLE \"only\"");
    }

    // ---------------------------------------------------------------- SECTION C
    // UNIQUE ... WITHOUT OVERLAPS, the other spelling of a temporal key.

    @Test
    void uniqueWithoutOverlapsIsATemporalKey() throws Exception {
        exec("CREATE TABLE dr_wo (id int, valid_at daterange,"
                + " CONSTRAINT dr_woc UNIQUE (id, valid_at WITHOUT OVERLAPS))");
        exec("INSERT INTO dr_wo VALUES (1,'[2020-01-01,2021-01-01)')");
        assertRejected("23P01", "conflicting key value violates exclusion constraint \"dr_woc\"",
                "INSERT INTO dr_wo VALUES (1,'[2020-06-01,2022-01-01)')");
        // Periods that merely touch do not overlap, and a different key never conflicts.
        assertAccepted("INSERT INTO dr_wo VALUES (1,'[2021-01-01,2022-01-01)')");
        assertAccepted("INSERT INTO dr_wo VALUES (2,'[2020-06-01,2022-01-01)')");
        assertEquals("3", scalar("SELECT count(*) FROM dr_wo"));
    }

    @Test
    void theUnnamedAndPrimaryKeySpellingsReadTheSame() throws Exception {
        assertAccepted("CREATE TABLE dr_wo2 (id int, valid_at daterange,"
                + " UNIQUE (id, valid_at WITHOUT OVERLAPS))");
        assertAccepted("CREATE TABLE dr_wo3 (id int, valid_at daterange,"
                + " PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))");
        exec("INSERT INTO dr_wo2 VALUES (1,'[2020-01-01,2021-01-01)')");
        assertRejected("23P01", "conflicting key value violates exclusion constraint",
                "INSERT INTO dr_wo2 VALUES (1,'[2020-06-01,2022-01-01)')");
    }

    @Test
    void anOrdinaryUniqueKeyIsUntouchedByTheNewSpelling() throws Exception {
        exec("CREATE TABLE dr_wo4 (id int, valid_at daterange, UNIQUE (id, valid_at))");
        exec("INSERT INTO dr_wo4 VALUES (1,'[2020-01-01,2021-01-01)')");
        // Overlapping is fine for a plain key; only an equal pair collides.
        assertAccepted("INSERT INTO dr_wo4 VALUES (1,'[2020-06-01,2022-01-01)')");
        assertRejected("23505", "duplicate key value",
                "INSERT INTO dr_wo4 VALUES (1,'[2020-01-01,2021-01-01)')");
        // A key over an expression, and a single-column key, still parse as they did.
        assertAccepted("CREATE TABLE dr_wo5 (id int, t text, UNIQUE (id))");
        assertAccepted("CREATE TABLE dr_wo6 (id int, t text, UNIQUE NULLS NOT DISTINCT (id, t))");
    }

    // ---------------------------------------------------------------- SECTION D
    // The two PG 18 catalog functions: an object that is not there is unknown, not an error.

    @Test
    void largeObjectPrivilegeAndAclAnswerNullForAnObjectThatIsNotThere() throws Exception {
        assertNull(scalar("SELECT has_largeobject_privilege(1, 'SELECT')"));
        assertNull(scalar("SELECT has_largeobject_privilege(1, 'UPDATE')"));
        assertNull(scalar("SELECT has_largeobject_privilege(current_user, 1, 'SELECT')"));
        assertNull(scalar("SELECT pg_get_acl('pg_class'::regclass, 1, 0)"));
        // A privilege name that names no privilege is still a caller's mistake, and so is a role.
        assertRejected("22023", "unrecognized privilege type: \"BOGUS\"",
                "SELECT has_largeobject_privilege(1, 'BOGUS')");
        assertRejected("42704", "role \"dr_norole\" does not exist",
                "SELECT has_largeobject_privilege('dr_norole', 1, 'SELECT')");
    }

    // ---------------------------------------------------------------- SECTION E
    // A partitioned parent holds no rows of its own, so UNLOGGED has nothing to mean.

    @Test
    void aPartitionedTableCannotBeUnlogged() throws Exception {
        assertRejected("0A000", "partitioned tables cannot be unlogged",
                "CREATE UNLOGGED TABLE dr_ul (i int) PARTITION BY RANGE (i)");
        // Unlogged without partitioning, partitioned without unlogged, and an unlogged partition
        // of a logged parent are all ordinary.
        assertAccepted("CREATE UNLOGGED TABLE dr_ul2 (i int PRIMARY KEY)");
        assertAccepted("CREATE TABLE dr_lp (i int) PARTITION BY RANGE (i)");
        assertAccepted("CREATE UNLOGGED TABLE dr_lp1 PARTITION OF dr_lp FOR VALUES FROM (1) TO (5)");
        exec("DROP TABLE dr_lp CASCADE");
    }

    // ---------------------------------------------------------------- SECTION F
    // TRUNCATE is blocked by the foreign key's existence, not by the rows behind it.

    @Test
    void truncateOfAReferencedTableIsRefusedWhileTheChildIsEmpty() throws Exception {
        exec("CREATE TABLE dr_fp (i int PRIMARY KEY)");
        exec("CREATE TABLE dr_fc (j int PRIMARY KEY, i int REFERENCES dr_fp(i))");
        SQLException e = assertRejected("0A000",
                "cannot truncate a table referenced in a foreign key constraint",
                "TRUNCATE dr_fp");
        // The Detail and the Hint travel as their own protocol fields; packed into the primary
        // message instead, a client reading getDetail() gets nothing. (pgjdbc renders both back
        // into getMessage() either way, which is why the fields are what has to be read here.)
        ServerErrorMessage sem = ((PSQLException) e).getServerErrorMessage();
        assertNotNull(sem);
        assertEquals("Table \"dr_fc\" references \"dr_fp\".", sem.getDetail());
        assertEquals("Truncate table \"dr_fc\" at the same time, or use TRUNCATE ... CASCADE.",
                sem.getHint());
        assertEquals("cannot truncate a table referenced in a foreign key constraint",
                sem.getMessage());
    }

    @Test
    void theWaysAroundTheTruncateRefusalAllStillWork() throws Exception {
        exec("CREATE TABLE dr_fp2 (i int PRIMARY KEY)");
        exec("CREATE TABLE dr_fc2 (j int PRIMARY KEY, i int REFERENCES dr_fp2(i))");
        exec("INSERT INTO dr_fp2 VALUES (1),(2)");
        exec("INSERT INTO dr_fc2 VALUES (10,1),(20,2)");
        assertAccepted("TRUNCATE dr_fp2, dr_fc2");
        assertEquals("0", scalar("SELECT count(*) FROM dr_fp2"));
        assertEquals("0", scalar("SELECT count(*) FROM dr_fc2"));
        assertAccepted("TRUNCATE dr_fp2 CASCADE");
        assertAccepted("TRUNCATE dr_fc2");
        // A self-referencing table is the whole graph by itself, and dropping the constraint
        // frees the parent again.
        exec("CREATE TABLE dr_self (i int PRIMARY KEY, p int REFERENCES dr_self(i))");
        assertAccepted("TRUNCATE dr_self");
        exec("ALTER TABLE dr_fc2 DROP CONSTRAINT dr_fc2_i_fkey");
        assertAccepted("TRUNCATE dr_fp2");
    }

    @Test
    void aPartitionedParentStillTruncatesItsWholeTree() throws Exception {
        exec("CREATE TABLE dr_pp (i int PRIMARY KEY) PARTITION BY RANGE (i)");
        exec("CREATE TABLE dr_pp1 PARTITION OF dr_pp FOR VALUES FROM (1) TO (10)");
        exec("INSERT INTO dr_pp VALUES (1),(2)");
        assertAccepted("TRUNCATE dr_pp");
        assertEquals("0", scalar("SELECT count(*) FROM dr_pp"));
    }

    // ---------------------------------------------------------------- SECTION G
    // A composite type owns a relation of its own name.

    @Test
    void renamingACompositeOntoATakenRelationNameIsARelationCollision() throws Exception {
        exec("CREATE TYPE dr_ct1 AS (a int)");
        exec("CREATE TYPE dr_ct2 AS (a int)");
        exec("CREATE TABLE dr_tbl (x int PRIMARY KEY)");
        exec("CREATE VIEW dr_vw AS SELECT x FROM dr_tbl");
        exec("CREATE SEQUENCE dr_sq");
        assertRejected("42P07", "relation \"dr_ct2\" already exists",
                "ALTER TYPE dr_ct1 RENAME TO dr_ct2");
        assertRejected("42P07", "relation \"dr_tbl\" already exists",
                "ALTER TYPE dr_ct1 RENAME TO dr_tbl");
        assertRejected("42P07", "relation \"dr_vw\" already exists",
                "ALTER TYPE dr_ct1 RENAME TO dr_vw");
        assertRejected("42P07", "relation \"dr_sq\" already exists",
                "ALTER TYPE dr_ct1 RENAME TO dr_sq");
    }

    @Test
    void aNameHeldByAnEnumOrADomainIsStillATypeCollision() throws Exception {
        exec("CREATE TYPE dr_ct4 AS (a int)");
        exec("CREATE TYPE dr_en AS ENUM ('x')");
        exec("CREATE DOMAIN dr_dm AS int");
        assertRejected("42710", "type \"dr_en\" already exists", "ALTER TYPE dr_ct4 RENAME TO dr_en");
        assertRejected("42710", "type \"dr_dm\" already exists", "ALTER TYPE dr_ct4 RENAME TO dr_dm");
        assertRejected("42710", "type \"dr_dm\" already exists", "ALTER TYPE dr_en RENAME TO dr_dm");
        // A rename onto a free name still works, and takes effect.
        assertAccepted("ALTER TYPE dr_ct4 RENAME TO dr_ct5");
        assertEquals(List.of("dr_ct5"), column(
                "SELECT typname FROM pg_type WHERE typname IN ('dr_ct4','dr_ct5') ORDER BY typname"));
    }

    // ---------------------------------------------------------------- SECTION H
    // ALTER naming a relation of the wrong kind.

    @Test
    void alterViewAndAlterSequenceReportTheWrongKindRatherThanAMissingRelation() throws Exception {
        exec("CREATE TABLE dr_k1 (a int PRIMARY KEY)");
        exec("CREATE VIEW dr_k1v AS SELECT a FROM dr_k1");
        exec("CREATE SEQUENCE dr_k1s");
        assertRejected("42809", "\"dr_k1\" is not a view", "ALTER VIEW dr_k1 RENAME TO dr_kx");
        assertRejected("42809", "\"dr_k1\" is not a sequence", "ALTER SEQUENCE dr_k1 RENAME TO dr_kx");
        assertRejected("42809", "\"dr_k1v\" is not a sequence", "ALTER SEQUENCE dr_k1v RENAME TO dr_kx");
        assertRejected("42809", "\"dr_k1s\" is not a view", "ALTER VIEW dr_k1s RENAME TO dr_kx");
        // A name that is nothing at all is still reported as missing.
        assertRejected("42P01", "relation \"dr_nosuchrel\" does not exist",
                "ALTER VIEW dr_nosuchrel RENAME TO dr_kx");
        assertRejected("42P01", "relation \"dr_nosuchrel\" does not exist",
                "ALTER SEQUENCE dr_nosuchrel RENAME TO dr_kx");
    }

    @Test
    void eachKindStillRenamesUnderItsOwnWord() throws Exception {
        exec("CREATE TABLE dr_k2 (a int PRIMARY KEY)");
        exec("CREATE VIEW dr_k2v AS SELECT a FROM dr_k2");
        exec("CREATE SEQUENCE dr_k2s");
        assertAccepted("ALTER VIEW dr_k2v RENAME TO dr_k2v2");
        assertAccepted("ALTER SEQUENCE dr_k2s RENAME TO dr_k2s2");
        assertAccepted("ALTER TABLE dr_k2v2 RENAME TO dr_k2v3");
        assertAccepted("ALTER SEQUENCE dr_k2s2 RESTART WITH 5");
        assertEquals("5", scalar("SELECT nextval('dr_k2s2')"));
    }

    @Test
    void alterIndexRenamesWhateverRelationTheNameOwns() throws Exception {
        // ALTER INDEX's rename is the generic relation rename and never looks at the kind, so a
        // table named here is renamed rather than reported as a missing relation.
        exec("CREATE TABLE dr_ai (a int PRIMARY KEY)");
        assertAccepted("ALTER INDEX dr_ai RENAME TO dr_ai2");
        assertEquals(List.of("dr_ai2"), column(
                "SELECT relname FROM pg_class WHERE relname IN ('dr_ai','dr_ai2') AND relkind = 'r'"));
        // An index of its own renames the same way; a name that is nothing is still missing.
        exec("CREATE TABLE dr_ait (a int PRIMARY KEY)");
        exec("CREATE INDEX dr_aix ON dr_ait (a)");
        assertAccepted("ALTER INDEX dr_aix RENAME TO dr_aix2");
        assertRejected("42P01", "relation \"dr_nosuchix\" does not exist",
                "ALTER INDEX dr_nosuchix RENAME TO dr_z");
    }

    // ---------------------------------------------------------------- SECTION I
    // Publications and statistics objects: an ALTER of one that is not there is a mistake, and a
    // rename onto a taken name must not destroy what holds it.

    @Test
    void alterPublicationRenamesAndReportsWhatIsNotThere() throws Exception {
        exec("CREATE TABLE dr_pt (a int PRIMARY KEY)");
        exec("CREATE PUBLICATION dr_pub1 FOR TABLE dr_pt");
        assertAccepted("ALTER PUBLICATION dr_pub1 RENAME TO dr_pub2");
        assertEquals(List.of("dr_pub2"), column(
                "SELECT pubname FROM pg_publication WHERE pubname LIKE 'dr_pub%' ORDER BY pubname"));
        assertRejected("42704", "publication \"dr_nosuchpub\" does not exist",
                "ALTER PUBLICATION dr_nosuchpub RENAME TO dr_x");
        assertRejected("42704", "publication \"dr_nosuchpub\" does not exist",
                "ALTER PUBLICATION dr_nosuchpub ADD TABLE dr_pt");
        exec("CREATE PUBLICATION dr_pub3 FOR TABLE dr_pt");
        assertRejected("42710", "publication \"dr_pub2\" already exists",
                "ALTER PUBLICATION dr_pub3 RENAME TO dr_pub2");
        // Both survive the refused rename, and the ordinary alterations still work.
        assertEquals(List.of("dr_pub2", "dr_pub3"), column(
                "SELECT pubname FROM pg_publication WHERE pubname LIKE 'dr_pub%' ORDER BY pubname"));
        // dr_pub3 was created FOR TABLE dr_pt, so adding it again would list it twice.
        assertRejected("42710", "relation \"dr_pt\" is already member of publication \"dr_pub3\"",
                "ALTER PUBLICATION dr_pub3 ADD TABLE dr_pt");
        assertAccepted("ALTER PUBLICATION dr_pub3 DROP TABLE dr_pt");
        // ...and once it is out, dropping it again names a relation the publication does not have.
        assertRejected("42704", "relation \"dr_pt\" is not part of the publication",
                "ALTER PUBLICATION dr_pub3 DROP TABLE dr_pt");
        assertAccepted("ALTER PUBLICATION dr_pub3 ADD TABLE dr_pt");
        assertAccepted("ALTER PUBLICATION dr_pub3 OWNER TO CURRENT_USER");
    }

    @Test
    void alterStatisticsRefusesToRenameOntoATakenName() throws Exception {
        exec("CREATE TABLE dr_st (a int PRIMARY KEY, b int)");
        exec("CREATE STATISTICS dr_s1 ON a, b FROM dr_st");
        exec("CREATE STATISTICS dr_s2 ON a, b FROM dr_st");
        assertRejected("42710", "statistics object \"dr_s2\" already exists in schema \"public\"",
                "ALTER STATISTICS dr_s1 RENAME TO dr_s2");
        // Neither object was destroyed by the refused rename.
        assertEquals(List.of("dr_s1", "dr_s2"), column(
                "SELECT stxname FROM pg_statistic_ext WHERE stxname LIKE 'dr_s%' ORDER BY stxname"));
        assertAccepted("ALTER STATISTICS dr_s1 RENAME TO dr_s3");
        assertAccepted("ALTER STATISTICS dr_s3 SET STATISTICS 100");
        assertEquals(List.of("dr_s2", "dr_s3"), column(
                "SELECT stxname FROM pg_statistic_ext WHERE stxname LIKE 'dr_s%' ORDER BY stxname"));
        assertRejected("42704", "statistics object \"dr_nosuchstat\" does not exist",
                "ALTER STATISTICS dr_nosuchstat RENAME TO dr_y");
    }

    @Test
    void alterOperatorNamesTheOperandTypes() {
        assertRejected("42883", "operator does not exist: integer === integer",
                "ALTER OPERATOR ===(int, int) OWNER TO CURRENT_USER");
        // A prefix operator has no left operand to name.
        assertRejected("42883", "operator does not exist: === integer",
                "ALTER OPERATOR ===(NONE, int) OWNER TO CURRENT_USER");
    }

    // ---------------------------------------------------------------- SECTION J
    // A renamed view column is the name the view answers to.

    @Test
    void renamingAViewColumnRenamesWhatTheViewAnswersTo() throws Exception {
        exec("CREATE TABLE dr_v2t (i int primary key, j int)");
        exec("INSERT INTO dr_v2t VALUES (1,10),(2,20)");
        exec("CREATE VIEW dr_v2 AS SELECT i, j FROM dr_v2t");
        exec("ALTER TABLE dr_v2 RENAME COLUMN i TO k");
        assertEquals(List.of("1", "2"), column("SELECT k FROM dr_v2 ORDER BY 1"));
        assertRejected("42703", "column \"i\" does not exist", "SELECT i FROM dr_v2 ORDER BY 1");
        assertEquals(List.of("k", "j"), labels("SELECT * FROM dr_v2"));
        // The catalog says the same thing the resolver does.
        assertEquals(List.of("k", "j"), column(
                "SELECT column_name FROM information_schema.columns"
                        + " WHERE table_name='dr_v2' ORDER BY ordinal_position"));
        // The base table keeps its own column names.
        assertEquals(List.of("1", "2"), column("SELECT i FROM dr_v2t ORDER BY 1"));
    }

    @Test
    void alterViewRenameColumnBehavesTheSame() throws Exception {
        exec("CREATE TABLE dr_rn_t (i int primary key)");
        exec("INSERT INTO dr_rn_t VALUES (7)");
        exec("CREATE VIEW dr_rn_v AS SELECT i FROM dr_rn_t");
        exec("ALTER TABLE dr_rn_v RENAME COLUMN i TO k");
        exec("ALTER VIEW dr_rn_v RENAME COLUMN k TO m");
        assertEquals(List.of("7"), column("SELECT m FROM dr_rn_v"));
        assertEquals(List.of("m"), labels("SELECT * FROM dr_rn_v"));
    }

    @Test
    void aViewOverAWildcardIsNotRelabelledByPosition() throws Exception {
        // There is no one target to write the name onto, so the query is left as it was rather
        // than relabelled wrongly, and the view keeps reading.
        exec("CREATE TABLE dr_ws (a int primary key, b int)");
        exec("INSERT INTO dr_ws VALUES (1,2)");
        exec("CREATE VIEW dr_wsv AS SELECT * FROM dr_ws");
        assertEquals(List.of("1"), column("SELECT a FROM dr_wsv"));
        assertEquals(List.of("a", "b"), labels("SELECT * FROM dr_wsv"));
    }

    // ---------------------------------------------------------------- SECTION K
    // date and timestamp have ends.

    @Test
    void dateArithmeticPastTheEndOfTheTypeIsOutOfRange() {
        assertRejected("22008", "date out of range", "SELECT DATE '5874897-12-31' + 1");
        assertRejected("22008", "date out of range", "SELECT DATE '2000-01-01' + 2147483647");
        assertRejected("22008", "date out of range", "SELECT DATE '2000-01-01' - 2147483647");
    }

    @Test
    void ordinaryDateArithmeticIsUntouched() throws Exception {
        assertEquals("2000-01-02", scalar("SELECT DATE '2000-01-01' + 1"));
        assertEquals("1999-12-02", scalar("SELECT DATE '2000-01-01' - 30"));
        assertEquals("5874897-12-31", scalar("SELECT DATE '5874897-12-31'"));
        assertEquals("5874897-12-30", scalar("SELECT DATE '5874897-12-31' - 1"));
        assertEquals("31", scalar("SELECT DATE '2000-02-01' - DATE '2000-01-01'"));
    }

    @Test
    void theWidestLegalTimestampsAreLegalInput() throws Exception {
        assertEquals("294276-12-31 23:59:59", scalar("SELECT TIMESTAMP '294276-12-31 23:59:59'"));
        assertEquals("294276-12-31 23:59:59.999999",
                scalar("SELECT TIMESTAMP '294276-12-31 23:59:59.999999'"));
        assertEquals("294276-12-31 00:00:00", scalar("SELECT TIMESTAMP '294276-12-31'"));
        assertEquals("294276-12-31 23:59:59", scalar("SELECT TIMESTAMP '294276-12-31T23:59:59'"));
        // Past the end it is the range that fails, not the spelling.
        assertRejected("22008", "timestamp out of range", "SELECT TIMESTAMP '294277-01-01 00:00:00'");
        // Ordinary timestamps are untouched.
        assertEquals("2024-03-04 05:06:07", scalar("SELECT TIMESTAMP '2024-03-04 05:06:07'"));
        assertEquals("2024-03-04 05:06:07.125", scalar("SELECT TIMESTAMP '2024-03-04 05:06:07.125'"));
    }

    // ---------------------------------------------------------------- SECTION L
    // The text-search ranking and highlighting functions are strict.

    @Test
    void rankingANullDocumentIsUnknownRatherThanZero() throws Exception {
        assertNull(scalar("SELECT ts_rank(to_tsvector('cat dog'), NULL::tsquery)"));
        assertNull(scalar("SELECT ts_rank(NULL::tsvector, to_tsquery('cat'))"));
        assertNull(scalar("SELECT ts_rank_cd(to_tsvector('cat dog'), NULL::tsquery)"));
        assertNull(scalar("SELECT ts_rank_cd(NULL::tsvector, to_tsquery('cat'))"));
        assertNull(scalar("SELECT ts_headline('cat dog', NULL::tsquery)"));
        assertNull(scalar("SELECT ts_headline(NULL::text, to_tsquery('cat'))"));
        assertNull(scalar("SELECT ts_headline('cat dog', to_tsquery('cat'), NULL)"));
        assertNull(scalar("SELECT ts_rewrite(to_tsquery('cat'), NULL::tsquery, to_tsquery('dog'))"));
    }

    @Test
    void withEverythingPresentTheyAnswerAsTheyAlwaysDid() throws Exception {
        assertEquals("t", scalar("SELECT ts_rank(to_tsvector('cat dog'), to_tsquery('cat')) > 0"));
        assertEquals("t", scalar("SELECT ts_rank_cd(to_tsvector('cat dog'), to_tsquery('cat')) > 0"));
        assertEquals("<b>cat</b> dog", scalar("SELECT ts_headline('cat dog', to_tsquery('cat'))"));
        assertEquals("'dog'", scalar(
                "SELECT ts_rewrite(to_tsquery('cat'), to_tsquery('cat'), to_tsquery('dog'))::text"));
    }

    // ---------------------------------------------------------------- SECTION M
    // bytea to integer reads the bytes rather than leaking a Java array identity.

    @Test
    void byteaCastsToTheIntegerItsBytesSpell() throws Exception {
        assertEquals("256", scalar("SELECT '\\x00000100'::bytea::int"));
        assertEquals("0", scalar("SELECT '\\x00000000'::bytea::int"));
        assertEquals("-1", scalar("SELECT '\\xffffffff'::bytea::int"));
    }
}
