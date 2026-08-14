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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The order objects are numbered in, and what a rolled-back definition change puts back.
 *
 * <p>Neither was what PostgreSQL says. A number was handed out before the block that hands the
 * numbers out in PostgreSQL's order had run, so a composite type, an enum and a domain each came
 * out ahead of their own array instead of behind it. A relation's row type was not numbered at
 * creation at all: it waited for whatever catalogue query first asked for it, which put every row
 * type behind every relation and in whatever order a hash map happened to iterate. A table's
 * implicit sequence was never announced to the register either, so it landed behind the row types
 * rather than ahead of its own table.
 *
 * <p>Nothing below reads an OID as a value -- PostgreSQL promises no particular number. Every
 * answer is one object's OID held against another's, which is the part PostgreSQL does promise
 * and the part that was wrong.
 *
 * <p>The other half is what a rollback owes. A DROP TABLE took the column's sequence, its
 * registration and its owner without writing any of that down, so rolling the drop back returned
 * a table whose serial column had no sequence behind it: an insert failed on the not-null
 * constraint, an identity column handed out 1 twice, and nextval conjured an unowned sequence the
 * next drop then left behind. A DROP TYPE ... CASCADE removed the indexes over the column it took
 * and the constraints that came with it, again without writing them down, so a rolled-back
 * CASCADE gave the column back unindexed and unconstrained: a duplicate went in where a unique
 * index still stood, and CREATE INDEX succeeded on a name PostgreSQL still holds.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class ObjectNumberingAndDefinitionRollbackTest {

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

    // ------------------------------------------------------------ helpers

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The first column of the first row, as text, or "(no rows)" when the query answers none. */
    private static String scalar(String sql) throws SQLException {
        return scalar(conn, sql);
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /** Every row the query answers: cells joined with "|", rows joined with ";". */
    private static String rows(String sql) throws SQLException {
        return rows(conn, sql);
    }

    private static String rows(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    String v = rs.getString(i);
                    sb.append(v == null ? "null" : v);
                }
                out.add(sb.toString());
            }
        }
        return String.join(";", out);
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
        return fieldsOf(conn, sql);
    }

    private static org.postgresql.util.ServerErrorMessage fieldsOf(Connection c, String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> {
            try (Statement st = c.createStatement()) {
                st.execute(sql);
            }
        }, "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    // ============================================================ how objects are numbered

    @Test
    void aCompositeTypeIsNumberedAfterItsRelationAndAfterItsArray() throws Exception {
        exec("CREATE TYPE zzt4e_comp AS (x int, y int)");
        // The composite's pg_class row is written first, then the array _zzt4e_comp, then the
        // type itself. The type coming first was the whole of the defect.
        assertEquals("t|t|t", rows(
                "SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type,"
                        + " c.oid < t.oid AS rel_before_type"
                        + " FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid"
                        + " JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_comp'"));
        exec("DROP TYPE zzt4e_comp");
    }

    @Test
    void aTableRowTypeIsNumberedAfterItsRelationAndAfterItsArray() throws Exception {
        exec("CREATE TABLE zzt4e_plain (i int)");
        assertEquals("t|t|t", rows(
                "SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type,"
                        + " c.oid < t.oid AS rel_before_type"
                        + " FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid"
                        + " JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_plain'"));
        exec("DROP TABLE zzt4e_plain");
    }

    @Test
    void anEnumAndADomainAreEachNumberedAfterTheirOwnArray() throws Exception {
        exec("CREATE TYPE zzt4e_enum AS ENUM ('a', 'b')");
        exec("CREATE DOMAIN zzt4e_dom AS int");
        assertEquals("t", rows("SELECT a.oid < t.oid AS array_before_type FROM pg_type t"
                + " JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_enum'"));
        assertEquals("t", rows("SELECT a.oid < t.oid AS array_before_type FROM pg_type t"
                + " JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_dom'"));
        exec("DROP DOMAIN zzt4e_dom");
        exec("DROP TYPE zzt4e_enum");
    }

    @Test
    void aRangeTypeIsTheLastOfTheFourNumbersItsDeclarationTakes() throws Exception {
        exec("CREATE TYPE zzt4e_rng AS RANGE (subtype = int4)");
        // _zzt4e_rng, then zzt4e_rng_multirange, then _zzt4e_rng_multirange, then zzt4e_rng.
        assertEquals("t|t|t", rows(
                "SELECT (SELECT oid FROM pg_type WHERE typname = '_zzt4e_rng')"
                        + " < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_rng_multirange') AS a1,"
                        + " (SELECT oid FROM pg_type WHERE typname = 'zzt4e_rng_multirange')"
                        + " < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_rng_multirange') AS a2,"
                        + " (SELECT oid FROM pg_type WHERE typname = '_zzt4e_rng_multirange')"
                        + " < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_rng') AS a3"));
        exec("DROP TYPE zzt4e_rng");
    }

    @Test
    void aViewAMaterializedViewAndAQueryWrittenTableEachCarryARowTypeOfTheirOwn() throws Exception {
        exec("CREATE VIEW zzt4e_v AS SELECT 1 AS a");
        exec("CREATE MATERIALIZED VIEW zzt4e_mv AS SELECT 1 AS a");
        exec("CREATE TABLE zzt4e_ctas AS SELECT 1 AS a");
        String q = "SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type"
                + " FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid"
                + " JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = '";
        assertEquals("t|t", rows(q + "zzt4e_v'"));
        assertEquals("t|t", rows(q + "zzt4e_mv'"));
        assertEquals("t|t", rows(q + "zzt4e_ctas'"));
        exec("DROP TABLE zzt4e_ctas");
        exec("DROP MATERIALIZED VIEW zzt4e_mv");
        exec("DROP VIEW zzt4e_v");
    }

    @Test
    void aPartitionedTableIsNumberedLikeAnyOtherRelationAndItsPartitionAfterIt() throws Exception {
        exec("CREATE TABLE zzt4e_part (i int, j int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt4e_p1 PARTITION OF zzt4e_part FOR VALUES FROM (0) TO (10)");
        assertEquals("t|t", rows(
                "SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type"
                        + " FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid"
                        + " JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_part'"));
        assertEquals("t", rows("SELECT (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_part')"
                + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_p1')"
                + " AS parent_rowtype_before_partition"));
        exec("DROP TABLE zzt4e_p1");
        exec("DROP TABLE zzt4e_part");
    }

    @Test
    void aSequenceAndAnIndexDescribeNoRowSoNeitherIsGivenARowType() throws Exception {
        exec("CREATE TABLE zzt4e_pl2 (i int)");
        exec("CREATE SEQUENCE zzt4e_seq");
        exec("CREATE INDEX zzt4e_ix_plain ON zzt4e_pl2 (i)");
        assertEquals("zzt4e_ix_plain|i|0;zzt4e_seq|S|0", rows(
                "SELECT relname, relkind, reltype FROM pg_class"
                        + " WHERE relname IN ('zzt4e_seq', 'zzt4e_ix_plain') ORDER BY relname"));
        assertEquals("0", scalar("SELECT count(*) FROM pg_type"
                + " WHERE typname IN ('zzt4e_seq', 'zzt4e_ix_plain')"));
        exec("DROP SEQUENCE zzt4e_seq");
        exec("DROP TABLE zzt4e_pl2");
    }

    @Test
    void anImplicitSequenceIsNumberedBeforeTheTableThatOwnsIt() throws Exception {
        exec("CREATE TABLE zzt4e_ser (i serial, j int)");
        // sequence, then table, then the array of the row type, then the row type.
        assertEquals("t|t|t", rows(
                "SELECT (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ser_i_seq')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ser') AS seq_before_table,"
                        + " (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ser')"
                        + " < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ser') AS table_before_array,"
                        + " (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ser')"
                        + " < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_ser') AS array_before_rowtype"));
        exec("DROP TABLE zzt4e_ser");
    }

    @Test
    void anIdentityColumnsSequenceIsNumberedExactlyAsASerialOneIs() throws Exception {
        exec("CREATE TABLE zzt4e_ident (i int GENERATED ALWAYS AS IDENTITY, j int)");
        assertEquals("t|t|t", rows(
                "SELECT (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ident_i_seq')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ident') AS seq_before_table,"
                        + " (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ident')"
                        + " < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ident') AS table_before_array,"
                        + " (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ident')"
                        + " < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_ident') AS array_before_rowtype"));
        exec("DROP TABLE zzt4e_ident");
    }

    @Test
    void twoSerialColumnsGiveTwoSequencesNumberedInColumnOrderBeforeTheTable() throws Exception {
        exec("CREATE TYPE zzt4e_early AS (q int)");
        exec("CREATE TABLE zzt4e_two (i serial, j serial)");
        assertEquals("t|t|t", rows(
                "SELECT (SELECT oid FROM pg_type WHERE typname = 'zzt4e_early')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_i_seq')"
                        + " AS earlier_type_before_later_sequence,"
                        + " (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_i_seq')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_j_seq')"
                        + " AS first_sequence_first,"
                        + " (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_j_seq')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two')"
                        + " AS both_sequences_before_table"));
        exec("DROP TABLE zzt4e_two");
        exec("DROP TYPE zzt4e_early");
    }

    @Test
    void aRowTypeIsNumberedBeforeEveryObjectCreatedAfterItsTable() throws Exception {
        // The row types used to be minted whenever a catalogue build first asked for them, which
        // put all of them behind every relation. Each of these was false then.
        exec("CREATE TABLE zzt4e_l1 (i int)");
        exec("CREATE TABLE zzt4e_l2 (i int)");
        exec("CREATE INDEX zzt4e_li ON zzt4e_l1 (i)");
        exec("CREATE SEQUENCE zzt4e_ls");
        exec("CREATE VIEW zzt4e_lv AS SELECT 1 AS a");
        exec("CREATE TYPE zzt4e_lc AS (q int)");
        assertEquals("t|t|t|t|t", rows(
                "SELECT (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_l1')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_l2') AS before_later_table,"
                        + " (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_l1')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_li') AS before_later_index,"
                        + " (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_l1')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ls') AS before_later_sequence,"
                        + " (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_l1')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_lv') AS before_later_view,"
                        + " (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_l1')"
                        + " < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_lc') AS before_later_type"));
        assertEquals("t|t", rows(
                "SELECT (SELECT typarray FROM pg_type WHERE typname = 'zzt4e_l1')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_l2')"
                        + " AS array_before_later_table,"
                        + " (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_l2')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_li')"
                        + " AS second_rowtype_before_later_index"));
        exec("DROP VIEW zzt4e_lv");
        exec("DROP TABLE zzt4e_l2");
        exec("DROP TABLE zzt4e_l1");
        exec("DROP SEQUENCE zzt4e_ls");
        exec("DROP TYPE zzt4e_lc");
    }

    @Test
    void aTableKeepsItsThreeNumbersThroughAChangeAndTakesLaterOnesWhenCreatedAgain()
            throws Exception {
        exec("CREATE TABLE zzt4e_cap (k text, relid bigint, rowtype bigint, arrtype bigint)");
        exec("CREATE TABLE zzt4e_keep (i int)");
        exec("INSERT INTO zzt4e_cap SELECT 'keep', c.oid, t.oid, a.oid FROM pg_class c"
                + " JOIN pg_type t ON t.oid = c.reltype JOIN pg_type a ON a.oid = t.typarray"
                + " WHERE c.relname = 'zzt4e_keep'");
        exec("ALTER TABLE zzt4e_keep ADD COLUMN j text");
        assertEquals("t|t|t", rows(
                "SELECT (SELECT relid FROM zzt4e_cap WHERE k = 'keep')"
                        + " = (SELECT oid FROM pg_class WHERE relname = 'zzt4e_keep') AS relation_oid_kept,"
                        + " (SELECT rowtype FROM zzt4e_cap WHERE k = 'keep')"
                        + " = (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_keep') AS rowtype_oid_kept,"
                        + " (SELECT arrtype FROM zzt4e_cap WHERE k = 'keep')"
                        + " = (SELECT typarray FROM pg_type WHERE typname = 'zzt4e_keep') AS array_oid_kept"));

        exec("DROP TABLE zzt4e_keep");
        exec("CREATE TABLE zzt4e_keep (i int)");
        assertEquals("t|t", rows(
                "SELECT (SELECT relid FROM zzt4e_cap WHERE k = 'keep')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_keep')"
                        + " AS recreated_relation_is_later,"
                        + " (SELECT rowtype FROM zzt4e_cap WHERE k = 'keep')"
                        + " < (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_keep')"
                        + " AS recreated_rowtype_is_later"));
        exec("DROP TABLE zzt4e_keep");
        exec("DROP TABLE zzt4e_cap");
    }

    @Test
    void aRowTypeAndItsArrayAreFiledInTheSchemaTheTableLivesIn() throws Exception {
        exec("CREATE SCHEMA zzt4e_sc");
        exec("CREATE TABLE zzt4e_sc.zzt4e_in (i serial)");
        assertEquals("t|t|t", rows(
                "SELECT (SELECT oid FROM pg_class WHERE relname = 'zzt4e_in_i_seq')"
                        + " < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_in') AS seq_before_table,"
                        + " (SELECT oid FROM pg_class WHERE relname = 'zzt4e_in')"
                        + " < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_in') AS table_before_array,"
                        + " (SELECT oid FROM pg_type WHERE typname = '_zzt4e_in')"
                        + " < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_in') AS array_before_rowtype"));
        assertEquals("t|t", rows(
                "SELECT t.typnamespace = n.oid AS rowtype_in_the_schema,"
                        + " a.typnamespace = n.oid AS array_in_the_schema"
                        + " FROM pg_type t JOIN pg_type a ON a.oid = t.typarray"
                        + " JOIN pg_namespace n ON n.nspname = 'zzt4e_sc' WHERE t.typname = 'zzt4e_in'"));
        exec("DROP SCHEMA zzt4e_sc CASCADE");
    }

    // ============================================================ what a rolled-back drop puts back

    @Test
    void aRolledBackDropPutsTheSerialSequenceBack() throws Exception {
        exec("CREATE TABLE zzt4e_rt (i serial, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_rt");
        exec("ROLLBACK");

        assertEquals("zzt4e_rt|r;zzt4e_rt_i_seq|S", rows(
                "SELECT relname, relkind FROM pg_class WHERE relname LIKE 'zzt4e_rt%' ORDER BY relname"));
        // The column still reads its default out of the sequence, and is still not nullable.
        assertEquals("nextval('zzt4e_rt_i_seq'::regclass)|NO", rows(
                "SELECT column_default, is_nullable FROM information_schema.columns"
                        + " WHERE table_name = 'zzt4e_rt' AND column_name = 'i'"));

        exec("INSERT INTO zzt4e_rt (s) VALUES ('a')");
        exec("INSERT INTO zzt4e_rt (s) VALUES ('b')");
        assertEquals("1|a;2|b", rows("SELECT i, s FROM zzt4e_rt ORDER BY i"));
        assertEquals("3", scalar("SELECT nextval('zzt4e_rt_i_seq')"));
        assertEquals("public.zzt4e_rt_i_seq", scalar("SELECT pg_get_serial_sequence('zzt4e_rt', 'i')"));

        // And the drop that is allowed to stand takes the sequence with it, leaving nothing behind.
        exec("DROP TABLE zzt4e_rt");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzt4e_rt%'"));
    }

    @Test
    void aRolledBackDropPutsABigserialSequenceBack() throws Exception {
        exec("CREATE TABLE zzt4e_bt (i bigserial, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_bt");
        exec("ROLLBACK");

        exec("INSERT INTO zzt4e_bt (s) VALUES ('a')");
        assertEquals("1", scalar("SELECT i FROM zzt4e_bt"));
        assertEquals("public.zzt4e_bt_i_seq", scalar("SELECT pg_get_serial_sequence('zzt4e_bt', 'i')"));
        exec("DROP TABLE zzt4e_bt");
    }

    @Test
    void aRolledBackDropPutsAnIdentityColumnsSequenceBack() throws Exception {
        exec("CREATE TABLE zzt4e_id (i int GENERATED BY DEFAULT AS IDENTITY, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_id");
        exec("ROLLBACK");

        // Without the sequence behind it the column handed out 1 twice.
        exec("INSERT INTO zzt4e_id (s) VALUES ('a')");
        exec("INSERT INTO zzt4e_id (s) VALUES ('b')");
        assertEquals("1|a;2|b", rows("SELECT i, s FROM zzt4e_id ORDER BY i"));
        assertEquals("zzt4e_id|r;zzt4e_id_i_seq|S", rows(
                "SELECT relname, relkind FROM pg_class WHERE relname LIKE 'zzt4e_id%' ORDER BY relname"));
        exec("DROP TABLE zzt4e_id");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzt4e_id%'"));
    }

    @Test
    void aRolledBackDropInASchemaPutsTheSequenceBackInThatSchema() throws Exception {
        exec("CREATE SCHEMA zzt4e_ss");
        exec("CREATE TABLE zzt4e_ss.zzt4e_st (i serial, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_ss.zzt4e_st");
        exec("ROLLBACK");

        assertEquals("zzt4e_ss|zzt4e_st_i_seq", rows(
                "SELECT sequence_schema, sequence_name FROM information_schema.sequences"
                        + " WHERE sequence_name LIKE 'zzt4e_st%'"));
        exec("INSERT INTO zzt4e_ss.zzt4e_st (s) VALUES ('a')");
        assertEquals("1", scalar("SELECT i FROM zzt4e_ss.zzt4e_st"));
        assertEquals("zzt4e_ss.zzt4e_st_i_seq",
                scalar("SELECT pg_get_serial_sequence('zzt4e_ss.zzt4e_st', 'i')"));
        exec("DROP SCHEMA zzt4e_ss CASCADE");
    }

    @Test
    void aSequenceAttachedWithOwnedByComesBackWithItsTable() throws Exception {
        exec("CREATE TABLE zzt4e_own (i int, s text)");
        exec("CREATE SEQUENCE zzt4e_ownseq");
        exec("ALTER SEQUENCE zzt4e_ownseq OWNED BY zzt4e_own.i");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_own");
        exec("ROLLBACK");

        assertEquals("1", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4e_ownseq'"));
        assertEquals("1", scalar("SELECT nextval('zzt4e_ownseq')"));
        // The ownership came back with it, so the next drop is still allowed to take it.
        exec("DROP TABLE zzt4e_own");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4e_ownseq'"));
    }

    @Test
    void aSequenceOnlyNamedByADefaultIsNotTakenByTheDropSoNothingIsRestoredForIt()
            throws Exception {
        exec("CREATE SEQUENCE zzt4e_loose");
        exec("CREATE TABLE zzt4e_free (i int DEFAULT nextval('zzt4e_loose'), s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_free");
        exec("ROLLBACK");

        assertEquals("1", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4e_loose'"));
        // The drop that stands does not take it either: it was never owned by the table.
        exec("DROP TABLE zzt4e_free");
        assertEquals("1", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4e_loose'"));
        exec("DROP SEQUENCE zzt4e_loose");
    }

    @Test
    void aRestoredSequenceKeepsTheIncrementAndThePositionItHad() throws Exception {
        exec("CREATE TABLE zzt4e_inc (i serial, s text)");
        exec("ALTER SEQUENCE zzt4e_inc_i_seq INCREMENT BY 5 RESTART WITH 100");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_inc");
        exec("ROLLBACK");

        // A sequence conjured fresh would start at 1 and count by 1.
        exec("INSERT INTO zzt4e_inc (s) VALUES ('a')");
        exec("INSERT INTO zzt4e_inc (s) VALUES ('b')");
        assertEquals("100|a;105|b", rows("SELECT i, s FROM zzt4e_inc ORDER BY i"));
        exec("DROP TABLE zzt4e_inc");
    }

    @Test
    void aRestoredSequenceIsStillOwnedSoItCannotBeDroppedOnItsOwn() throws Exception {
        exec("CREATE TABLE zzt4e_dep (i serial, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_dep");
        exec("ROLLBACK");

        org.postgresql.util.ServerErrorMessage m = fieldsOf("DROP SEQUENCE zzt4e_dep_i_seq");
        assertEquals("2BP01", m.getSQLState());
        assertEquals("cannot drop sequence zzt4e_dep_i_seq because other objects depend on it",
                m.getMessage());
        assertEquals("default value for column i of table zzt4e_dep depends on sequence"
                + " zzt4e_dep_i_seq", m.getDetail());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", m.getHint());
        assertEquals("1", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4e_dep_i_seq'"));
        exec("DROP TABLE zzt4e_dep");
    }

    @Test
    void rollbackToASavepointPutsTheSequenceBack() throws Exception {
        exec("CREATE TABLE zzt4e_sp (i serial, s text)");
        exec("INSERT INTO zzt4e_sp (s) VALUES ('one')");
        exec("BEGIN");
        exec("SAVEPOINT a");
        exec("DROP TABLE zzt4e_sp");
        exec("ROLLBACK TO SAVEPOINT a");
        exec("COMMIT");

        assertEquals("zzt4e_sp|r;zzt4e_sp_i_seq|S", rows(
                "SELECT relname, relkind FROM pg_class WHERE relname LIKE 'zzt4e_sp%' ORDER BY relname"));
        exec("INSERT INTO zzt4e_sp (s) VALUES ('two')");
        assertEquals("1|one;2|two", rows("SELECT i, s FROM zzt4e_sp ORDER BY i"));
        exec("DROP TABLE zzt4e_sp");
    }

    @Test
    void aCommittedDropTakesTheSequenceWithIt() throws Exception {
        exec("CREATE TABLE zzt4e_cm (i serial, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_cm");
        exec("COMMIT");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzt4e_cm%'"));
    }

    @Test
    void aTableDroppedAndWrittenAgainInOneTransactionKeepsTheSequenceItWasWrittenWith()
            throws Exception {
        exec("CREATE TABLE zzt4e_re (i serial, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_re");
        exec("CREATE TABLE zzt4e_re (i serial, s text)");
        exec("COMMIT");

        exec("INSERT INTO zzt4e_re (s) VALUES ('a')");
        assertEquals("1|a", rows("SELECT i, s FROM zzt4e_re ORDER BY i"));
        assertEquals("zzt4e_re|r;zzt4e_re_i_seq|S", rows(
                "SELECT relname, relkind FROM pg_class WHERE relname LIKE 'zzt4e_re%' ORDER BY relname"));
        exec("DROP TABLE zzt4e_re");
    }

    @Test
    void aRolledBackSchemaDropCascadePutsTheSequenceBack() throws Exception {
        exec("CREATE SCHEMA zzt4e_cs");
        exec("CREATE TABLE zzt4e_cs.zzt4e_cst (i serial, s text)");
        exec("BEGIN");
        exec("DROP SCHEMA zzt4e_cs CASCADE");
        exec("ROLLBACK");

        assertEquals("zzt4e_cs|zzt4e_cst_i_seq", rows(
                "SELECT sequence_schema, sequence_name FROM information_schema.sequences"
                        + " WHERE sequence_name = 'zzt4e_cst_i_seq'"));
        exec("INSERT INTO zzt4e_cs.zzt4e_cst (s) VALUES ('a')");
        assertEquals("1", scalar("SELECT i FROM zzt4e_cs.zzt4e_cst"));
        exec("DROP SCHEMA zzt4e_cs CASCADE");
    }

    @Test
    void aRestoredSequenceIsStillTheOneTheColumnDefaultNames() throws Exception {
        exec("CREATE TABLE zzt4e_own2 (i int, s text)");
        exec("CREATE SEQUENCE zzt4e_own2seq");
        exec("ALTER SEQUENCE zzt4e_own2seq OWNED BY zzt4e_own2.i");
        exec("ALTER TABLE zzt4e_own2 ALTER COLUMN i SET DEFAULT nextval('zzt4e_own2seq')");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_own2");
        exec("ROLLBACK");

        assertEquals("public.zzt4e_own2seq", scalar("SELECT pg_get_serial_sequence('zzt4e_own2', 'i')"));
        exec("INSERT INTO zzt4e_own2 (s) VALUES ('a')");
        assertEquals("1|a", rows("SELECT i, s FROM zzt4e_own2"));
        exec("DROP TABLE zzt4e_own2");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzt4e_own2%'"));
    }

    @Test
    void anotherSessionSeesTheRestoredSequence() throws Exception {
        exec("CREATE TABLE zzt4e_ts (i serial, s text)");
        exec("BEGIN");
        exec("DROP TABLE zzt4e_ts");
        exec("ROLLBACK");

        try (Connection other = open()) {
            assertEquals("zzt4e_ts|r;zzt4e_ts_i_seq|S", rows(other,
                    "SELECT relname, relkind FROM pg_class WHERE relname LIKE 'zzt4e_ts%'"
                            + " ORDER BY relname"));
            try (Statement st = other.createStatement()) {
                st.execute("INSERT INTO zzt4e_ts (s) VALUES ('from b')");
            }
            assertEquals("public.zzt4e_ts_i_seq",
                    scalar(other, "SELECT pg_get_serial_sequence('zzt4e_ts', 'i')"));
        }
        // and the session that rolled the drop back goes on from where the other one left off
        exec("INSERT INTO zzt4e_ts (s) VALUES ('from a')");
        assertEquals("1|from b;2|from a", rows("SELECT i, s FROM zzt4e_ts ORDER BY i"));
        exec("DROP TABLE zzt4e_ts");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzt4e_ts%'"));
    }

    // ============================================================ a rolled-back DROP TYPE CASCADE

    @Test
    void aRolledBackTypeDropCascadePutsEveryIndexBack() throws Exception {
        exec("CREATE TYPE zzt4e_e2 AS ENUM ('a', 'b')");
        exec("CREATE TABLE zzt4e_ti (id int, m zzt4e_e2, k int)");
        exec("CREATE INDEX zzt4e_ix1 ON zzt4e_ti (m)");
        exec("CREATE INDEX zzt4e_ix2 ON zzt4e_ti (k)");
        exec("CREATE UNIQUE INDEX zzt4e_ix3 ON zzt4e_ti (id, m)");
        exec("INSERT INTO zzt4e_ti VALUES (1, 'a', 7)");
        exec("BEGIN");
        exec("DROP TYPE zzt4e_e2 CASCADE");
        exec("ROLLBACK");

        assertEquals("zzt4e_ix1;zzt4e_ix2;zzt4e_ix3", rows(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'zzt4e_ti' ORDER BY indexname"));
        assertEquals("zzt4e_ix1|i;zzt4e_ix2|i;zzt4e_ix3|i", rows(
                "SELECT relname, relkind FROM pg_class WHERE relname LIKE 'zzt4e_ix%' ORDER BY relname"));
        assertEquals("1|a|7", rows("SELECT id, m::text AS m, k FROM zzt4e_ti ORDER BY id"));
        assertEquals("{a,b}", scalar("SELECT enum_range(NULL::zzt4e_e2)::text"));

        // The unique index is a real one again: a duplicate is refused, with the index named.
        org.postgresql.util.ServerErrorMessage dup = fieldsOf("INSERT INTO zzt4e_ti VALUES (1, 'a', 9)");
        assertEquals("23505", dup.getSQLState());
        assertEquals("duplicate key value violates unique constraint \"zzt4e_ix3\"", dup.getMessage());
        assertEquals("Key (id, m)=(1, a) already exists.", dup.getDetail());
        assertEquals("zzt4e_ix3", dup.getConstraint());

        // And the name is still taken, so it cannot be created a second time.
        org.postgresql.util.ServerErrorMessage taken =
                fieldsOf("CREATE INDEX zzt4e_ix1 ON zzt4e_ti (m)");
        assertEquals("42P07", taken.getSQLState());
        assertEquals("relation \"zzt4e_ix1\" already exists", taken.getMessage());

        // The column the CASCADE took is back where it was, under the type it was declared with.
        assertEquals("id|1|integer;m|2|zzt4e_e2;k|3|integer", rows(
                "SELECT a.attname, a.attnum, format_type(a.atttypid, a.atttypmod) AS t"
                        + " FROM pg_attribute a WHERE a.attrelid = 'zzt4e_ti'::regclass"
                        + " AND a.attnum > 0 ORDER BY a.attnum"));
        exec("INSERT INTO zzt4e_ti VALUES (2, 'b', 8)");
        assertEquals("1|a;2|b", rows("SELECT id, m::text AS m FROM zzt4e_ti ORDER BY m, id"));

        exec("DROP TABLE zzt4e_ti");
        exec("DROP TYPE zzt4e_e2");
    }

    @Test
    void aRolledBackTypeDropCascadeRestoresTheIndexEntriesAndNotJustTheirNames() throws Exception {
        exec("CREATE TYPE zzt4e_e6 AS ENUM ('a', 'b')");
        exec("CREATE TABLE zzt4e_ti6 (id int, m zzt4e_e6, k int)");
        exec("CREATE INDEX zzt4e_ix6a ON zzt4e_ti6 (m)");
        exec("CREATE UNIQUE INDEX zzt4e_ix6b ON zzt4e_ti6 (id, m)");
        exec("BEGIN");
        exec("DROP TYPE zzt4e_e6 CASCADE");
        exec("ROLLBACK");

        assertEquals("zzt4e_ix6a|f;zzt4e_ix6b|t", rows(
                "SELECT c.relname, i.indisunique FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
                        + " WHERE c.relname LIKE 'zzt4e_ix6%' ORDER BY c.relname"));
        // A restored index is a real entry, so it can be dropped like any other.
        exec("DROP INDEX zzt4e_ix6a");
        assertEquals("zzt4e_ix6b", rows(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'zzt4e_ti6' ORDER BY indexname"));

        exec("INSERT INTO zzt4e_ti6 VALUES (1, 'a', 1)");
        assertEquals("23505", stateOf("INSERT INTO zzt4e_ti6 VALUES (1, 'a', 2)"));
        assertEquals("1", scalar("SELECT count(*) FROM zzt4e_ti6"));

        exec("DROP TABLE zzt4e_ti6");
        exec("DROP TYPE zzt4e_e6");
    }

    @Test
    void aRolledBackTypeDropCascadePutsEveryConstraintBack() throws Exception {
        exec("CREATE TYPE zzt4e_e3 AS ENUM ('a', 'b', 'c')");
        exec("CREATE TABLE zzt4e_pa (id int PRIMARY KEY,"
                + " m zzt4e_e3 NOT NULL DEFAULT 'a' CHECK (m <> 'c'),"
                + " u zzt4e_e3 UNIQUE, arr zzt4e_e3[])");
        exec("CREATE TABLE zzt4e_ch (id int, u zzt4e_e3 REFERENCES zzt4e_pa (u))");
        exec("CREATE VIEW zzt4e_vw AS SELECT id, m FROM zzt4e_pa");
        exec("INSERT INTO zzt4e_pa VALUES (1, 'a', 'a', '{a,b}')");
        exec("BEGIN");
        exec("DROP TYPE zzt4e_e3 CASCADE");
        exec("ROLLBACK");

        assertEquals("zzt4e_pa_id_not_null|n;zzt4e_pa_m_check|c;zzt4e_pa_m_not_null|n;"
                        + "zzt4e_pa_pkey|p;zzt4e_pa_u_key|u",
                rows("SELECT conname, contype FROM pg_constraint"
                        + " WHERE conrelid = 'zzt4e_pa'::regclass ORDER BY conname"));

        org.postgresql.util.ServerErrorMessage pk = fieldsOf("INSERT INTO zzt4e_pa (id, m) VALUES (1, 'b')");
        assertEquals("23505", pk.getSQLState());
        assertEquals("duplicate key value violates unique constraint \"zzt4e_pa_pkey\"", pk.getMessage());
        assertEquals("Key (id)=(1) already exists.", pk.getDetail());

        org.postgresql.util.ServerErrorMessage nn = fieldsOf("INSERT INTO zzt4e_pa (id, m) VALUES (2, NULL)");
        assertEquals("23502", nn.getSQLState());
        assertEquals("null value in column \"m\" of relation \"zzt4e_pa\" violates not-null constraint",
                nn.getMessage());
        assertEquals("Failing row contains (2, null, null, null).", nn.getDetail());
        assertEquals("m", nn.getColumn());

        org.postgresql.util.ServerErrorMessage ck = fieldsOf("INSERT INTO zzt4e_pa (id, m) VALUES (3, 'c')");
        assertEquals("23514", ck.getSQLState());
        assertEquals("new row for relation \"zzt4e_pa\" violates check constraint \"zzt4e_pa_m_check\"",
                ck.getMessage());
        assertEquals("Failing row contains (3, c, null, null).", ck.getDetail());
        assertEquals("zzt4e_pa_m_check", ck.getConstraint());

        org.postgresql.util.ServerErrorMessage uq = fieldsOf("INSERT INTO zzt4e_pa (id, u) VALUES (4, 'a')");
        assertEquals("23505", uq.getSQLState());
        assertEquals("duplicate key value violates unique constraint \"zzt4e_pa_u_key\"", uq.getMessage());
        assertEquals("Key (u)=(a) already exists.", uq.getDetail());

        org.postgresql.util.ServerErrorMessage fk = fieldsOf("INSERT INTO zzt4e_ch VALUES (1, 'b')");
        assertEquals("23503", fk.getSQLState());
        assertEquals("insert or update on table \"zzt4e_ch\" violates foreign key constraint"
                + " \"zzt4e_ch_u_fkey\"", fk.getMessage());
        assertEquals("Key (u)=(b) is not present in table \"zzt4e_pa\".", fk.getDetail());
        assertEquals("zzt4e_ch_u_fkey", fk.getConstraint());

        // The default is back too, and so are the rows and the view over them.
        exec("INSERT INTO zzt4e_pa (id) VALUES (5)");
        assertEquals("1|a|a|{a,b};5|a|null|null", rows(
                "SELECT id, m::text AS m, u::text AS u, arr::text AS arr FROM zzt4e_pa ORDER BY id"));
        assertEquals("1|a;5|a", rows("SELECT id, m::text AS m FROM zzt4e_vw ORDER BY id"));

        exec("DROP TABLE zzt4e_ch");
        exec("DROP VIEW zzt4e_vw");
        exec("DROP TABLE zzt4e_pa");
        exec("DROP TYPE zzt4e_e3");
    }

    @Test
    void aRolledBackTypeDropCascadePutsTheEnumItsLabelsAndTheirOrderBack() throws Exception {
        exec("CREATE TYPE zzt4e_e4 AS ENUM ('a', 'b', 'c')");
        exec("CREATE TABLE zzt4e_lt (id int, m zzt4e_e4)");
        exec("INSERT INTO zzt4e_lt VALUES (1, 'c'), (2, 'a'), (3, 'b')");
        exec("BEGIN");
        exec("DROP TYPE zzt4e_e4 CASCADE");
        exec("ROLLBACK");

        assertEquals("{a,b,c}", scalar("SELECT enum_range(NULL::zzt4e_e4)::text"));
        assertEquals("a|1;b|2;c|3", rows(
                "SELECT e.enumlabel, e.enumsortorder FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid"
                        + " WHERE t.typname = 'zzt4e_e4' ORDER BY e.enumsortorder"));
        // The column sorts by the labels' declared order, not by their text.
        assertEquals("2|a;3|b;1|c", rows("SELECT id, m::text AS m FROM zzt4e_lt ORDER BY m"));

        exec("DROP TABLE zzt4e_lt");
        exec("DROP TYPE zzt4e_e4");
    }

    @Test
    void aRolledBackTypeDropCascadePutsBackTheFunctionThatTakesTheType() throws Exception {
        exec("CREATE TYPE zzt4e_e9 AS ENUM ('a', 'b')");
        exec("CREATE TABLE zzt4e_t9 (id int, m zzt4e_e9)");
        exec("CREATE FUNCTION zzt4e_f9(x zzt4e_e9) RETURNS text LANGUAGE sql"
                + " AS $$ SELECT x::text || '!' $$");
        exec("INSERT INTO zzt4e_t9 VALUES (1, 'b')");
        exec("BEGIN");
        exec("DROP TYPE zzt4e_e9 CASCADE");
        exec("ROLLBACK");

        assertEquals("a!", scalar("SELECT zzt4e_f9('a')"));
        assertEquals("1|b!", rows("SELECT id, zzt4e_f9(m) AS f FROM zzt4e_t9 ORDER BY id"));
        // It is back under the argument type it was declared with, not under a text one.
        assertEquals("zzt4e_f9|x zzt4e_e9", rows(
                "SELECT p.proname, pg_get_function_arguments(p.oid) AS args FROM pg_proc p"
                        + " WHERE p.proname = 'zzt4e_f9'"));

        exec("DROP FUNCTION zzt4e_f9(zzt4e_e9)");
        exec("DROP TABLE zzt4e_t9");
        exec("DROP TYPE zzt4e_e9");
    }

    @Test
    void aRolledBackTypeDropCascadePutsBackTheDependentsThatBlockAPlainDrop() throws Exception {
        exec("CREATE TYPE zzt4e_e5 AS ENUM ('a', 'b', 'c')");
        exec("CREATE TABLE zzt4e_pa5 (id int PRIMARY KEY,"
                + " m zzt4e_e5 NOT NULL DEFAULT 'a' CHECK (m <> 'c'), arr zzt4e_e5[])");
        exec("CREATE DOMAIN zzt4e_d5 AS zzt4e_e5");
        exec("CREATE VIEW zzt4e_vw5 AS SELECT id, m FROM zzt4e_pa5");
        exec("INSERT INTO zzt4e_pa5 VALUES (1, 'a', '{a,b}')");
        exec("BEGIN");
        exec("DROP TYPE zzt4e_e5 CASCADE");
        exec("ROLLBACK");

        assertEquals("zzt4e_d5|USER-DEFINED", rows(
                "SELECT domain_name, data_type FROM information_schema.domains"
                        + " WHERE domain_name = 'zzt4e_d5'"));

        // Every dependent is back, so the type cannot be dropped without CASCADE any more.
        org.postgresql.util.ServerErrorMessage m = fieldsOf("DROP TYPE zzt4e_e5");
        assertEquals("2BP01", m.getSQLState());
        assertEquals("cannot drop type zzt4e_e5 because other objects depend on it", m.getMessage());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", m.getHint());
        assertEquals("1", scalar("SELECT count(*) FROM pg_type WHERE typname = 'zzt4e_e5'"));
        assertEquals("zzt4e_pa5;zzt4e_vw5", rows(
                "SELECT c.relname FROM pg_class c WHERE c.relname IN ('zzt4e_pa5', 'zzt4e_vw5')"
                        + " ORDER BY c.relname"));

        exec("DROP VIEW zzt4e_vw5");
        exec("DROP TABLE zzt4e_pa5");
        exec("DROP DOMAIN zzt4e_d5");
        exec("DROP TYPE zzt4e_e5");
    }

    @Test
    void anotherSessionSeesTheRestoredIndex() throws Exception {
        exec("CREATE TYPE zzt4e_e7 AS ENUM ('a', 'b')");
        exec("CREATE TABLE zzt4e_t7 (id int, m zzt4e_e7)");
        exec("CREATE UNIQUE INDEX zzt4e_ix7 ON zzt4e_t7 (id, m)");
        exec("INSERT INTO zzt4e_t7 VALUES (1, 'a')");
        exec("BEGIN");
        exec("DROP TYPE zzt4e_e7 CASCADE");
        exec("ROLLBACK");

        try (Connection other = open()) {
            assertEquals("zzt4e_ix7", rows(other,
                    "SELECT indexname FROM pg_indexes WHERE tablename = 'zzt4e_t7' ORDER BY indexname"));
            org.postgresql.util.ServerErrorMessage dup =
                    fieldsOf(other, "INSERT INTO zzt4e_t7 VALUES (1, 'a')");
            assertEquals("23505", dup.getSQLState());
            assertEquals("duplicate key value violates unique constraint \"zzt4e_ix7\"", dup.getMessage());
            assertEquals("Key (id, m)=(1, a) already exists.", dup.getDetail());
            assertEquals("1|a", rows(other, "SELECT id, m::text AS m FROM zzt4e_t7 ORDER BY id"));
        }

        exec("DROP TABLE zzt4e_t7");
        exec("DROP TYPE zzt4e_e7");
    }
}
