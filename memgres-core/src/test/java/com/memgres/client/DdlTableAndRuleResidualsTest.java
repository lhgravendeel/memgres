package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Residuals in three neighbouring places, all measured against PostgreSQL 18.
 *
 * <p>{@code CREATE TABLE b (LIKE a)} copied the source column whole, so a's defaults, identity
 * and generation expression came with it and a NOT NULL constraint was renamed after the new
 * table rather than keeping the source's name. A statement's target columns were resolved one
 * row at a time, so an UPDATE naming a column that is not there succeeded over an empty table.
 * An index definition's collation, operator class and storage parameters were never looked at,
 * nor were a table's. And a DO ALSO rule on UPDATE or DELETE never fired at all, while a rule
 * with several actions ran none of them.
 */
class DdlTableAndRuleResidualsTest {

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
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
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

    // =========================================================================
    // LIKE copies the shape of a column and nothing else unless asked
    // =========================================================================

    @Test
    void likeWithoutIncludingDefaultsCopiesNoDefault() throws Exception {
        exec("DROP TABLE IF EXISTS btu_lb CASCADE");
        exec("DROP TABLE IF EXISTS btu_la CASCADE");
        exec("CREATE TABLE btu_la (i int, j text DEFAULT 'q', k int DEFAULT 4, "
                + "d date DEFAULT DATE '2020-01-01', n numeric(5,2) DEFAULT 1.5, b bool DEFAULT true)");
        exec("CREATE TABLE btu_lb (LIKE btu_la)");
        exec("INSERT INTO btu_lb DEFAULT VALUES");

        assertEquals(List.of("null|null|null|null|null|null"),
                rows("SELECT i, j, k, d, n, b FROM btu_lb"));
        assertEquals(List.of("i|null", "j|null", "k|null", "d|null", "n|null", "b|null"),
                rows("SELECT column_name, column_default FROM information_schema.columns"
                        + " WHERE table_name='btu_lb' ORDER BY ordinal_position"));
        assertEquals(List.of("0"),
                rows("SELECT count(*) FROM pg_attrdef WHERE adrelid = 'btu_lb'::regclass"));
    }

    @Test
    void includingDefaultsBringsThemBack() throws Exception {
        exec("DROP TABLE IF EXISTS btu_lb CASCADE");
        exec("DROP TABLE IF EXISTS btu_la CASCADE");
        exec("CREATE TABLE btu_la (i int, j text DEFAULT 'q', k int DEFAULT 4)");
        exec("CREATE TABLE btu_lb (LIKE btu_la INCLUDING DEFAULTS)");
        exec("INSERT INTO btu_lb DEFAULT VALUES");

        assertEquals(List.of("null|q|4"), rows("SELECT i, j, k FROM btu_lb"));
    }

    @Test
    void includingAllBringsTheDefaultsToo() throws Exception {
        exec("DROP TABLE IF EXISTS btu_lb CASCADE");
        exec("DROP TABLE IF EXISTS btu_la CASCADE");
        exec("CREATE TABLE btu_la (i int PRIMARY KEY, j text DEFAULT 'q')");
        exec("CREATE TABLE btu_lb (LIKE btu_la INCLUDING ALL)");
        exec("INSERT INTO btu_lb (i) VALUES (1)");

        assertEquals(List.of("q"), rows("SELECT j FROM btu_lb"));
    }

    @Test
    void aLaterExcludingTakesBackWhatIncludingAllBrought() throws Exception {
        exec("DROP TABLE IF EXISTS btu_lb CASCADE");
        exec("DROP TABLE IF EXISTS btu_la CASCADE");
        exec("CREATE TABLE btu_la (i int PRIMARY KEY, j text DEFAULT 'q')");
        exec("CREATE TABLE btu_lb (LIKE btu_la INCLUDING ALL EXCLUDING DEFAULTS)");

        assertEquals(List.of("i|null", "j|null"),
                rows("SELECT column_name, column_default FROM information_schema.columns"
                        + " WHERE table_name='btu_lb' ORDER BY ordinal_position"));
    }

    @Test
    void aCopiedNotNullKeepsTheSourceTablesName() throws Exception {
        exec("DROP TABLE IF EXISTS btu_lk2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_lk CASCADE");
        exec("CREATE TABLE btu_lk (i int PRIMARY KEY, j text CHECK (j <> ''), k int DEFAULT 4)");
        exec("CREATE TABLE btu_lk2 (LIKE btu_lk)");

        assertEquals(List.of("btu_lk_i_not_null|n"),
                rows("SELECT conname, contype FROM pg_constraint"
                        + " WHERE conrelid = 'btu_lk2'::regclass ORDER BY conname"));

        exec("DROP TABLE btu_lk2");
        exec("CREATE TABLE btu_lk2 (LIKE btu_lk INCLUDING ALL)");
        assertEquals(List.of("btu_lk2_pkey|p", "btu_lk_i_not_null|n", "btu_lk_j_check|c"),
                rows("SELECT conname, contype FROM pg_constraint"
                        + " WHERE conrelid = 'btu_lk2'::regclass ORDER BY conname"));
    }

    @Test
    void anIdentityAndAGeneratedColumnTravelOnlyWhenNamed() throws Exception {
        exec("DROP TABLE IF EXISTS btu_gi2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_gi CASCADE");
        exec("CREATE TABLE btu_gi (i int GENERATED BY DEFAULT AS IDENTITY, j int)");
        exec("CREATE TABLE btu_gi2 (LIKE btu_gi)");
        assertEquals(List.of("i|NO|null", "j|NO|null"),
                rows("SELECT column_name, is_identity, identity_generation"
                        + " FROM information_schema.columns WHERE table_name='btu_gi2'"
                        + " ORDER BY ordinal_position"));

        exec("DROP TABLE btu_gi2");
        exec("CREATE TABLE btu_gi2 (LIKE btu_gi INCLUDING IDENTITY)");
        assertEquals(List.of("i|YES|BY DEFAULT"),
                rows("SELECT column_name, is_identity, identity_generation"
                        + " FROM information_schema.columns"
                        + " WHERE table_name='btu_gi2' AND column_name='i'"));

        exec("DROP TABLE IF EXISTS btu_gg2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_gg CASCADE");
        exec("CREATE TABLE btu_gg (i int, j int GENERATED ALWAYS AS (i * 2) STORED)");
        exec("CREATE TABLE btu_gg2 (LIKE btu_gg)");
        assertEquals(List.of("i|NEVER", "j|NEVER"),
                rows("SELECT column_name, is_generated FROM information_schema.columns"
                        + " WHERE table_name='btu_gg2' ORDER BY ordinal_position"));

        exec("DROP TABLE btu_gg2");
        exec("CREATE TABLE btu_gg2 (LIKE btu_gg INCLUDING GENERATED)");
        assertEquals(List.of("i|NEVER", "j|ALWAYS"),
                rows("SELECT column_name, is_generated FROM information_schema.columns"
                        + " WHERE table_name='btu_gg2' ORDER BY ordinal_position"));
    }

    @Test
    void aSerialsSequenceDefaultDoesNotTravelAndTheTypeIsPlain() throws Exception {
        exec("DROP TABLE IF EXISTS btu_sr2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_sr CASCADE");
        exec("CREATE TABLE btu_sr (i serial, j int)");
        exec("CREATE TABLE btu_sr2 (LIKE btu_sr)");

        assertEquals(List.of("i|integer|null", "j|integer|null"),
                rows("SELECT column_name, data_type, column_default"
                        + " FROM information_schema.columns WHERE table_name='btu_sr2'"
                        + " ORDER BY ordinal_position"));
    }

    @Test
    void aColumnCommentTravelsOnlyWithIncludingComments() throws Exception {
        exec("DROP TABLE IF EXISTS btu_cm2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_cm CASCADE");
        exec("CREATE TABLE btu_cm (i int, j text)");
        exec("COMMENT ON COLUMN btu_cm.i IS 'hello'");
        exec("CREATE TABLE btu_cm2 (LIKE btu_cm)");
        assertEquals(List.of("null"), rows("SELECT col_description('btu_cm2'::regclass, 1)"));

        exec("DROP TABLE btu_cm2");
        exec("CREATE TABLE btu_cm2 (LIKE btu_cm INCLUDING COMMENTS)");
        assertEquals(List.of("hello"), rows("SELECT col_description('btu_cm2'::regclass, 1)"));
    }

    @Test
    void everyIncludingOptionStillParsesAndTheColumnsArriveWhole() throws Exception {
        exec("DROP TABLE IF EXISTS btu_o CASCADE");
        exec("CREATE TABLE btu_o (a varchar(9), b numeric(6,3), c int[], d timestamptz)");
        for (String option : new String[]{"DEFAULTS", "CONSTRAINTS", "INDEXES", "IDENTITY",
                "GENERATED", "COMMENTS", "STATISTICS", "STORAGE", "COMPRESSION", "ALL"}) {
            exec("DROP TABLE IF EXISTS btu_o2 CASCADE");
            assertEquals("OK", stateOf("CREATE TABLE btu_o2 (LIKE btu_o INCLUDING " + option + ")"),
                    "INCLUDING " + option);
            assertEquals(List.of("a|character varying|9", "b|numeric|null",
                            "c|ARRAY|null", "d|timestamp with time zone|null"),
                    rows("SELECT column_name, data_type, character_maximum_length"
                            + " FROM information_schema.columns WHERE table_name='btu_o2'"
                            + " ORDER BY ordinal_position"),
                    "INCLUDING " + option);
            exec("DROP TABLE btu_o2");
            assertEquals("OK", stateOf("CREATE TABLE btu_o2 (LIKE btu_o EXCLUDING " + option + ")"),
                    "EXCLUDING " + option);
        }
    }

    // =========================================================================
    // A statement's names are resolved before any row is looked at
    // =========================================================================

    @Test
    void anAssignmentToAMissingColumnIsRefusedOverAnEmptyTable() throws Exception {
        exec("DROP TABLE IF EXISTS btu_up CASCADE");
        exec("CREATE TABLE btu_up (i int PRIMARY KEY, j text)");

        assertEquals("42703", stateOf("UPDATE btu_up SET nosuch = 1"));
        assertEquals("column \"nosuch\" of relation \"btu_up\" does not exist",
                messageOf("UPDATE btu_up SET nosuch = 1"));
        assertEquals("column \"nosuch\" of relation \"btu_up\" does not exist",
                messageOf("UPDATE btu_up SET nosuch = 1 WHERE i = 99"));
        assertEquals("column \"nosuch\" of relation \"btu_up\" does not exist",
                messageOf("UPDATE btu_up AS t SET nosuch = 1"));
        assertEquals("column \"btu_up\" of relation \"btu_up\" does not exist",
                messageOf("UPDATE btu_up SET btu_up.i = 1"));
    }

    @Test
    void anInsertColumnListNamesTheRelationInThePostgresWording() throws Exception {
        exec("DROP TABLE IF EXISTS btu_up CASCADE");
        exec("CREATE TABLE btu_up (i int PRIMARY KEY, j text)");
        exec("INSERT INTO btu_up VALUES (1,'a')");

        assertEquals("column \"nosuch\" of relation \"btu_up\" does not exist",
                messageOf("INSERT INTO btu_up (nosuch) VALUES (1)"));
        assertEquals("column \"nosuch\" of relation \"btu_up\" does not exist",
                messageOf("INSERT INTO btu_up (i, nosuch) VALUES (5, 1)"));
        assertEquals("column \"nosuch\" of relation \"btu_up\" does not exist",
                messageOf("INSERT INTO btu_up (nosuch) SELECT 1"));
    }

    @Test
    void aWhereOrAnAssignedValueIsResolvedJustAsEarly() throws Exception {
        exec("DROP TABLE IF EXISTS btu_up CASCADE");
        exec("CREATE TABLE btu_up (i int PRIMARY KEY, j text)");

        assertEquals("column \"nosuch\" does not exist",
                messageOf("UPDATE btu_up SET i = 1 WHERE nosuch = 2"));
        assertEquals("column \"nosuch\" does not exist", messageOf("UPDATE btu_up SET i = nosuch"));
        assertEquals("column \"nosuch\" does not exist",
                messageOf("DELETE FROM btu_up WHERE nosuch = 1"));
        assertEquals("column t.nosuch does not exist",
                messageOf("UPDATE btu_up AS t SET i = 1 WHERE t.nosuch = 2"));
    }

    @Test
    void aViewIsNamedByTheNameTheStatementWrote() throws Exception {
        exec("DROP VIEW IF EXISTS btu_vv CASCADE");
        exec("DROP TABLE IF EXISTS btu_vt CASCADE");
        exec("CREATE TABLE btu_vt (i int PRIMARY KEY, j text)");
        exec("CREATE VIEW btu_vv AS SELECT i, j FROM btu_vt");

        assertEquals("column \"nosuch\" of relation \"btu_vv\" does not exist",
                messageOf("UPDATE btu_vv SET nosuch = 1"));
        assertEquals("column \"nosuch\" of relation \"btu_vv\" does not exist",
                messageOf("INSERT INTO btu_vv (nosuch) VALUES (1)"));
    }

    @Test
    void theOrdinaryShapesOverAnEmptyTableStillRun() throws Exception {
        exec("DROP TABLE IF EXISTS btu_up CASCADE");
        exec("CREATE TABLE btu_up (i int PRIMARY KEY, j text)");

        for (String sql : new String[]{
                "UPDATE btu_up SET j = 'z' WHERE i = 1",
                "UPDATE btu_up SET j = 'z' WHERE btu_up.i = 1",
                "UPDATE btu_up AS t SET j = 'z' WHERE t.i = 1",
                "UPDATE btu_up SET i = i + 1 WHERE j LIKE 'a%'",
                "UPDATE btu_up SET j = upper(j) WHERE length(j) > 0",
                "UPDATE btu_up SET j = CASE WHEN i > 0 THEN 'p' ELSE 'n' END",
                "UPDATE btu_up SET j = i::text WHERE i::text = '1'",
                "UPDATE btu_up SET i = 1 WHERE i IN (1,2,3)",
                "UPDATE btu_up SET i = (SELECT max(i) FROM btu_up)",
                "UPDATE btu_up SET i = 1 WHERE EXISTS (SELECT 1 FROM btu_up x WHERE x.i = btu_up.i)",
                "UPDATE btu_up SET j = DEFAULT",
                "UPDATE btu_up SET j = 'z' RETURNING i",
                "UPDATE btu_up SET j = t.z FROM (SELECT 'q' AS z) t WHERE btu_up.i = 1",
                "DELETE FROM btu_up WHERE i = 1",
                "DELETE FROM btu_up WHERE j IS NULL",
                "INSERT INTO btu_up (i, j) SELECT 4, 'v'"}) {
            assertEquals("OK", stateOf(sql), sql);
        }
    }

    @Test
    void aSystemColumnIsStillItsOwnComplaint() throws Exception {
        exec("DROP TABLE IF EXISTS btu_up CASCADE");
        exec("CREATE TABLE btu_up (i int PRIMARY KEY, j text)");
        exec("INSERT INTO btu_up VALUES (1,'a')");

        assertEquals("0A000", stateOf("UPDATE btu_up SET ctid = '(0,1)' WHERE i = 1"));
        assertEquals("0A000", stateOf("UPDATE btu_up SET xmin = 0 WHERE i = 1"));
    }

    // =========================================================================
    // Index and table definition options
    // =========================================================================

    @Test
    void anIndexCollationHasToExist() throws Exception {
        exec("DROP TABLE IF EXISTS btu_ix CASCADE");
        exec("CREATE TABLE btu_ix (a int, txt text)");

        assertEquals("42704", stateOf("CREATE INDEX btu_i1 ON btu_ix (txt COLLATE \"nosuchcollation\")"));
        assertEquals("collation \"nosuchcollation\" for encoding \"UTF8\" does not exist",
                messageOf("CREATE INDEX btu_i1 ON btu_ix (txt COLLATE \"nosuchcollation\")"));
        assertEquals("OK", stateOf("CREATE INDEX btu_i2 ON btu_ix (txt COLLATE \"C\")"));
        assertEquals("OK", stateOf("CREATE INDEX btu_i3 ON btu_ix (txt COLLATE \"POSIX\")"));
    }

    @Test
    void anIndexOperatorClassHasToExistForTheAccessMethod() throws Exception {
        exec("DROP TABLE IF EXISTS btu_ix CASCADE");
        exec("CREATE TABLE btu_ix (a int, txt text)");

        assertEquals("operator class \"nosuchopclass\" does not exist for access method \"btree\"",
                messageOf("CREATE INDEX btu_i4 ON btu_ix (a nosuchopclass)"));
        assertEquals("OK", stateOf("CREATE INDEX btu_i5 ON btu_ix (a int4_ops)"));
        assertEquals("OK", stateOf("CREATE INDEX btu_i6 ON btu_ix (txt text_pattern_ops)"));
    }

    @Test
    void anIndexStorageParameterIsCheckedAgainstItsAccessMethod() throws Exception {
        exec("DROP TABLE IF EXISTS btu_ix CASCADE");
        exec("CREATE TABLE btu_ix (a int, txt text)");

        assertEquals("value 0 out of bounds for option \"fillfactor\"",
                messageOf("CREATE INDEX btu_i7 ON btu_ix USING btree (a) WITH (fillfactor = 0)"));
        assertEquals("value 200 out of bounds for option \"fillfactor\"",
                messageOf("CREATE INDEX btu_i7 ON btu_ix USING btree (a) WITH (fillfactor = 200)"));
        assertEquals("22023",
                stateOf("CREATE INDEX btu_i7 ON btu_ix USING btree (a) WITH (fillfactor = 5)"));
        assertEquals("unrecognized parameter \"nosuchoption\"",
                messageOf("CREATE INDEX btu_i7 ON btu_ix USING btree (a) WITH (nosuchoption = 1)"));
        assertEquals("invalid value for boolean option \"deduplicate_items\": 7",
                messageOf("CREATE INDEX btu_i7 ON btu_ix USING btree (a) WITH (deduplicate_items = 7)"));
        // deduplicate_items belongs to btree and to no other method
        assertEquals("unrecognized parameter \"deduplicate_items\"",
                messageOf("CREATE INDEX btu_i7 ON btu_ix USING hash (a) WITH (deduplicate_items = on)"));
    }

    @Test
    void aTablesStorageParametersAreCheckedTheSameWay() throws Exception {
        exec("DROP TABLE IF EXISTS btu_tf CASCADE");

        assertEquals("value 200 out of bounds for option \"fillfactor\"",
                messageOf("CREATE TABLE btu_tf (a int) WITH (fillfactor = 200)"));
        assertEquals("value 0 out of bounds for option \"fillfactor\"",
                messageOf("CREATE TABLE btu_tf (a int) WITH (fillfactor = 0)"));
        assertEquals("unrecognized parameter \"nosuchoption\"",
                messageOf("CREATE TABLE btu_tf (a int) WITH (nosuchoption = 1)"));
        // ...and a refused definition leaves nothing behind
        assertEquals(List.of("0"),
                rows("SELECT count(*) FROM information_schema.tables WHERE table_name='btu_tf'"));

        assertEquals("OK", stateOf("CREATE TABLE btu_tf (a int)"
                + " WITH (fillfactor = 70, autovacuum_enabled = false)"));
        assertEquals("value 200 out of bounds for option \"fillfactor\"",
                messageOf("ALTER TABLE btu_tf SET (fillfactor = 200)"));
        assertEquals("unrecognized parameter \"nosuchoption\"",
                messageOf("ALTER TABLE btu_tf SET (nosuchoption = 1)"));
        assertEquals("OK", stateOf("ALTER TABLE btu_tf SET (fillfactor = 80)"));
        assertEquals("OK", stateOf("ALTER TABLE btu_tf SET (autovacuum_vacuum_scale_factor = 0.1)"));
        assertEquals("OK", stateOf("ALTER TABLE btu_tf RESET (fillfactor)"));
    }

    @Test
    void anAlteredIndexParameterIsCheckedToo() throws Exception {
        exec("DROP TABLE IF EXISTS btu_ix CASCADE");
        exec("CREATE TABLE btu_ix (a int, txt text)");
        exec("CREATE INDEX btu_ia ON btu_ix (a)");

        assertEquals("value 200 out of bounds for option \"fillfactor\"",
                messageOf("ALTER INDEX btu_ia SET (fillfactor = 200)"));
        assertEquals("unrecognized parameter \"nosuchoption\"",
                messageOf("ALTER INDEX btu_ia SET (nosuchoption = 1)"));
        assertEquals("OK", stateOf("ALTER INDEX btu_ia SET (fillfactor = 80)"));
        assertEquals("OK", stateOf("ALTER INDEX btu_ia SET (deduplicate_items = off)"));
        assertEquals("OK", stateOf("ALTER INDEX btu_ia RESET (fillfactor)"));
    }

    @Test
    void everyOrdinaryIndexFormStillBuilds() throws Exception {
        exec("DROP TABLE IF EXISTS btu_ix CASCADE");
        exec("CREATE TABLE btu_ix (a int, txt text)");

        for (String sql : new String[]{
                "CREATE INDEX btu_x1 ON btu_ix (a DESC NULLS LAST, txt ASC NULLS FIRST)",
                "CREATE INDEX btu_x2 ON btu_ix (a) INCLUDE (txt)",
                "CREATE INDEX btu_x3 ON btu_ix (a) WHERE a > 0",
                "CREATE INDEX btu_x4 ON btu_ix ((a + 1))",
                "CREATE INDEX btu_x5 ON btu_ix (lower(txt))",
                "CREATE UNIQUE INDEX btu_x6 ON btu_ix (a)",
                "CREATE INDEX btu_x7 ON btu_ix USING btree (a) WITH (fillfactor = 100)",
                "CREATE INDEX btu_x8 ON btu_ix USING btree (a) WITH (fillfactor = 10)",
                "CREATE INDEX btu_x9 ON btu_ix USING hash (a) WITH (fillfactor = 50)",
                "CREATE INDEX btu_x10 ON btu_ix USING btree (txt COLLATE \"C\" text_pattern_ops"
                        + " DESC NULLS FIRST) INCLUDE (a) WITH (fillfactor = 80)"
                        + " WHERE txt IS NOT NULL"}) {
            assertEquals("OK", stateOf(sql), sql);
        }
        assertEquals(List.of("CREATE INDEX btu_x5 ON public.btu_ix USING btree (lower(txt))"),
                rows("SELECT indexdef FROM pg_indexes WHERE indexname = 'btu_x5'"));
    }

    @Test
    void aTablesIndexesGoWithIt() throws Exception {
        exec("DROP TABLE IF EXISTS btu_ix CASCADE");
        exec("CREATE TABLE btu_ix (a int, txt text)");
        exec("CREATE INDEX btu_ia ON btu_ix (a)");
        exec("DROP TABLE btu_ix");

        assertEquals(List.of("0"),
                rows("SELECT count(*) FROM pg_indexes WHERE indexname = 'btu_ia'"));
        // ...so the same table and index can be created again
        exec("CREATE TABLE btu_ix (a int, txt text)");
        assertEquals("OK", stateOf("CREATE INDEX btu_ia ON btu_ix (a)"));
    }

    // =========================================================================
    // Rules
    // =========================================================================

    @Test
    void aDoAlsoRuleOnUpdateAndOnDeleteFires() throws Exception {
        exec("DROP TABLE IF EXISTS btu_rt CASCADE");
        exec("DROP TABLE IF EXISTS btu_rlog CASCADE");
        exec("CREATE TABLE btu_rt (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rlog (m text)");
        exec("CREATE RULE btu_ri AS ON INSERT TO btu_rt DO ALSO INSERT INTO btu_rlog VALUES ('ins')");
        exec("CREATE RULE btu_ru AS ON UPDATE TO btu_rt DO ALSO INSERT INTO btu_rlog VALUES ('upd')");
        exec("CREATE RULE btu_rd AS ON DELETE TO btu_rt DO ALSO INSERT INTO btu_rlog VALUES ('del')");

        exec("INSERT INTO btu_rt VALUES (1,1)");
        exec("UPDATE btu_rt SET b = 2 WHERE a = 1");
        exec("DELETE FROM btu_rt WHERE a = 1");

        assertEquals(List.of("del|1", "ins|1", "upd|1"),
                rows("SELECT m, count(*) FROM btu_rlog GROUP BY m ORDER BY m"));
    }

    @Test
    void anActionThatNeverSaysOldOrNewRunsOncePerStatement() throws Exception {
        exec("DROP TABLE IF EXISTS btu_rt CASCADE");
        exec("DROP TABLE IF EXISTS btu_rlog CASCADE");
        exec("CREATE TABLE btu_rt (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rlog (m text)");
        exec("INSERT INTO btu_rt VALUES (1,1),(2,2)");
        exec("CREATE RULE btu_ru AS ON UPDATE TO btu_rt DO ALSO INSERT INTO btu_rlog VALUES ('u')");

        exec("UPDATE btu_rt SET b = b + 1");
        assertEquals(List.of("1"), rows("SELECT count(*) FROM btu_rlog"));

        // ...and not at all when the statement touched no row
        exec("DELETE FROM btu_rlog");
        exec("UPDATE btu_rt SET b = 5 WHERE a = 99");
        assertEquals(List.of("0"), rows("SELECT count(*) FROM btu_rlog"));
    }

    @Test
    void anActionThatReadsOldOrNewRunsOncePerRow() throws Exception {
        exec("DROP TABLE IF EXISTS btu_rt CASCADE");
        exec("DROP TABLE IF EXISTS btu_rlog CASCADE");
        exec("CREATE TABLE btu_rt (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rlog (o int, n int)");
        exec("CREATE RULE btu_ru AS ON UPDATE TO btu_rt"
                + " DO ALSO INSERT INTO btu_rlog VALUES (OLD.b, NEW.b)");
        exec("INSERT INTO btu_rt VALUES (1,10),(2,20)");

        exec("UPDATE btu_rt SET b = b + 1");
        assertEquals(List.of("10|11", "20|21"), rows("SELECT o, n FROM btu_rlog ORDER BY o"));
    }

    @Test
    void aQualifiedRuleFiresOnlyForTheRowsItsWhereHoldsFor() throws Exception {
        exec("DROP TABLE IF EXISTS btu_rc CASCADE");
        exec("DROP TABLE IF EXISTS btu_rclog CASCADE");
        exec("CREATE TABLE btu_rc (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rclog (m int)");
        exec("CREATE RULE btu_rcr AS ON UPDATE TO btu_rc WHERE NEW.b > 5"
                + " DO ALSO INSERT INTO btu_rclog VALUES (NEW.b)");
        exec("INSERT INTO btu_rc VALUES (1,1),(2,2)");

        exec("UPDATE btu_rc SET b = 9 WHERE a = 1");
        exec("UPDATE btu_rc SET b = 2 WHERE a = 2");

        assertEquals(List.of("9"), rows("SELECT m FROM btu_rclog ORDER BY m"));
    }

    @Test
    void aRuleWithSeveralActionsRunsThemAll() throws Exception {
        exec("DROP TABLE IF EXISTS btu_r2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_rlog2 CASCADE");
        exec("CREATE TABLE btu_r2 (a int PRIMARY KEY)");
        exec("CREATE TABLE btu_rlog2 (m text)");
        exec("INSERT INTO btu_r2 VALUES (1),(2)");
        exec("CREATE RULE btu_r4 AS ON UPDATE TO btu_r2"
                + " DO ( INSERT INTO btu_rlog2 VALUES ('m1'); INSERT INTO btu_rlog2 VALUES ('m2'); )");

        exec("UPDATE btu_r2 SET a = a + 10 WHERE a = 1");
        assertEquals(List.of("m1|1", "m2|1"),
                rows("SELECT m, count(*) FROM btu_rlog2 GROUP BY m ORDER BY m"));
    }

    @Test
    void severalActionsComeBackParenthesisedFromPgRules() throws Exception {
        exec("DROP TABLE IF EXISTS btu_r2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_rlog2 CASCADE");
        exec("CREATE TABLE btu_r2 (a int PRIMARY KEY)");
        exec("CREATE TABLE btu_rlog2 (m text)");
        exec("CREATE RULE btu_r4 AS ON UPDATE TO btu_r2"
                + " DO ( INSERT INTO btu_rlog2 VALUES ('m1'); INSERT INTO btu_rlog2 VALUES ('m2'); )");

        assertEquals(List.of("CREATE RULE btu_r4 AS\n"
                        + "    ON UPDATE TO public.btu_r2 DO ( INSERT INTO btu_rlog2 (m)\n"
                        + "  VALUES ('m1'::text);\n"
                        + " INSERT INTO btu_rlog2 (m)\n"
                        + "  VALUES ('m2'::text);\n"
                        + ");"),
                rows("SELECT definition FROM pg_rules WHERE tablename = 'btu_r2'"));
    }

    @Test
    void aSingleActionAndAQualificationAreWrittenTheWayPostgresDoes() throws Exception {
        exec("DROP TABLE IF EXISTS btu_rc CASCADE");
        exec("DROP TABLE IF EXISTS btu_rclog CASCADE");
        exec("CREATE TABLE btu_rc (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rclog (m int)");
        exec("CREATE RULE btu_rcr AS ON UPDATE TO btu_rc WHERE NEW.b > 5"
                + " DO ALSO INSERT INTO btu_rclog VALUES (NEW.b)");

        assertEquals(List.of("CREATE RULE btu_rcr AS\n"
                        + "    ON UPDATE TO public.btu_rc\n"
                        + "   WHERE (new.b > 5) DO  INSERT INTO btu_rclog (m)\n"
                        + "  VALUES (new.b);"),
                rows("SELECT definition FROM pg_rules WHERE tablename = 'btu_rc'"));
    }

    @Test
    void insteadKeepsItsWordAndInsteadNothingKeepsItsShape() throws Exception {
        exec("DROP TABLE IF EXISTS btu_ris CASCADE");
        exec("DROP TABLE IF EXISTS btu_rislog CASCADE");
        exec("CREATE TABLE btu_ris (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rislog (m text)");
        exec("CREATE RULE btu_risr AS ON UPDATE TO btu_ris"
                + " DO INSTEAD INSERT INTO btu_rislog VALUES ('x')");

        assertEquals(List.of("CREATE RULE btu_risr AS\n"
                        + "    ON UPDATE TO public.btu_ris DO INSTEAD  INSERT INTO btu_rislog (m)\n"
                        + "  VALUES ('x'::text);"),
                rows("SELECT definition FROM pg_rules WHERE tablename = 'btu_ris'"));

        exec("DROP TABLE IF EXISTS btu_rin CASCADE");
        exec("CREATE TABLE btu_rin (a int PRIMARY KEY, b int)");
        exec("INSERT INTO btu_rin VALUES (1,1)");
        exec("CREATE RULE btu_rinr AS ON UPDATE TO btu_rin DO INSTEAD NOTHING");
        exec("UPDATE btu_rin SET b = 5");

        assertEquals(List.of("1|1"), rows("SELECT a, b FROM btu_rin"));
        assertEquals(List.of("CREATE RULE btu_rinr AS\n"
                        + "    ON UPDATE TO public.btu_rin DO INSTEAD NOTHING;"),
                rows("SELECT definition FROM pg_rules WHERE tablename = 'btu_rin'"));
    }

    @Test
    void aDroppedRuleStopsFiring() throws Exception {
        exec("DROP TABLE IF EXISTS btu_rd2 CASCADE");
        exec("DROP TABLE IF EXISTS btu_rdlog CASCADE");
        exec("CREATE TABLE btu_rd2 (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rdlog (m text)");
        exec("INSERT INTO btu_rd2 VALUES (1,1)");
        exec("CREATE RULE btu_rdr AS ON UPDATE TO btu_rd2 WHERE NEW.b > 0"
                + " DO ALSO INSERT INTO btu_rdlog VALUES ('u')");
        exec("DROP RULE btu_rdr ON btu_rd2");
        // The old rule's qualification must not survive to filter a new one
        exec("CREATE RULE btu_rdr AS ON UPDATE TO btu_rd2 DO ALSO INSERT INTO btu_rdlog VALUES ('v')");
        exec("UPDATE btu_rd2 SET b = -1");

        assertEquals(List.of("v"), rows("SELECT m FROM btu_rdlog ORDER BY m"));
    }

    @Test
    void anAlsoRuleLeavesTheStatementsOwnWorkAlone() throws Exception {
        exec("DROP TABLE IF EXISTS btu_rw CASCADE");
        exec("DROP TABLE IF EXISTS btu_rwlog CASCADE");
        exec("CREATE TABLE btu_rw (a int PRIMARY KEY, b int)");
        exec("CREATE TABLE btu_rwlog (m text)");
        exec("INSERT INTO btu_rw VALUES (1,1),(2,2)");
        exec("CREATE RULE btu_rwr AS ON UPDATE TO btu_rw DO ALSO INSERT INTO btu_rwlog VALUES ('u')");

        try (Statement s = conn.createStatement()) {
            assertEquals(2, s.executeUpdate("UPDATE btu_rw SET b = b + 1"));
        }
        assertEquals(List.of("2", "3"), rows("SELECT b FROM btu_rw ORDER BY a"));
    }
}
