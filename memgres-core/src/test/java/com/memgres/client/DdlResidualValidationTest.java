package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A schema definition is written once and lived with for years, so the checks PostgreSQL makes
 * while it is being written are what stop a migration from succeeding here and failing there.
 *
 * <p>These tests pin the residual ones: which relations share a name, what a view really depends
 * on, whether the object an ALTER names exists at all, whether the rows already stored can satisfy
 * a rule about to be declared over them, and the order a multi-action ALTER TABLE settles its
 * actions in. Each group also pins the neighbouring definitions that must keep being accepted —
 * a rule that fires on correct SQL costs more than the permissiveness it removes.
 */
class DdlResidualValidationTest {

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
        try (Statement st = conn.createStatement()) { st.execute(sql); }
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

    /** Assert the statement is refused with this SQLSTATE and a message containing this text. */
    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    /** Assert the statement is accepted. Used for the shapes that must not start failing. */
    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    // ---------------------------------------------------------------- SECTION A
    // DROP TABLE asks what the view reads, not what its text says.

    @Test
    void aNameInsideAViewsTextIsNotADependency() throws Exception {
        exec("CREATE TABLE dv_lit (a int PRIMARY KEY)");
        exec("CREATE TABLE dv_lit_src (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_lit_v AS SELECT a, 'dv_lit' AS why FROM dv_lit_src");
        assertAccepted("DROP TABLE dv_lit");
    }

    @Test
    void aRelationWhoseNameStartsTheSameWayIsADifferentRelation() throws Exception {
        exec("CREATE TABLE dv_pre (a int PRIMARY KEY)");
        exec("CREATE TABLE dv_pre_long (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_pre_v AS SELECT a FROM dv_pre_long");
        assertAccepted("DROP TABLE dv_pre");
    }

    @Test
    void aColumnOfThatNameElsewhereIsNotADependency() throws Exception {
        exec("CREATE TABLE dv_col (a int PRIMARY KEY)");
        exec("CREATE TABLE dv_colhost (id int PRIMARY KEY, dv_col int)");
        exec("CREATE VIEW dv_col_v AS SELECT dv_col FROM dv_colhost");
        assertAccepted("DROP TABLE dv_col");
    }

    @Test
    void aWithOfThatNameShadowsTheTableForTheWholeQuery() throws Exception {
        exec("CREATE TABLE dv_cte (a int PRIMARY KEY)");
        exec("CREATE TABLE dv_cte_src (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_cte_v AS WITH dv_cte AS (SELECT a FROM dv_cte_src) SELECT * FROM dv_cte");
        assertAccepted("DROP TABLE dv_cte");
    }

    @Test
    void aSameNamedTableInAnotherSchemaIsADifferentRelation() throws Exception {
        exec("CREATE SCHEMA dv_s");
        exec("CREATE TABLE dv_s.dv_sch (a int PRIMARY KEY)");
        exec("CREATE TABLE dv_sch (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_sch_v AS SELECT a FROM dv_s.dv_sch");
        assertAccepted("DROP TABLE dv_sch");
    }

    @Test
    void aDependencyAlreadyDroppedNoLongerBlocks() throws Exception {
        exec("CREATE TABLE dv_gone (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_gone_v AS SELECT a FROM dv_gone");
        exec("DROP VIEW dv_gone_v");
        assertAccepted("DROP TABLE dv_gone");
    }

    @Test
    void aRealDependencyStillBlocksTheDrop() throws Exception {
        exec("CREATE TABLE dv_real (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_real_v AS SELECT z.a FROM dv_real AS z");
        assertRejected("2BP01", "cannot drop table dv_real because other objects depend on it",
                "DROP TABLE dv_real");
        exec("CREATE TABLE dv_sub (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_sub_v AS SELECT (SELECT count(*) FROM dv_sub) AS c");
        assertRejected("2BP01", "cannot drop table dv_sub because other objects depend on it",
                "DROP TABLE dv_sub");
    }

    @Test
    void cascadeReachesTheViewOverTheViewToo() throws Exception {
        exec("CREATE TABLE dv_t1 (a int PRIMARY KEY)");
        exec("CREATE VIEW dv_t1_v1 AS SELECT a FROM dv_t1");
        exec("CREATE VIEW dv_t1_v2 AS SELECT a FROM dv_t1_v1");
        assertRejected("2BP01", "cannot drop table dv_t1 because other objects depend on it",
                "DROP TABLE dv_t1");
        exec("DROP TABLE dv_t1 CASCADE");
        assertEquals("0", scalar("SELECT count(*)::int FROM information_schema.views"
                + " WHERE table_name IN ('dv_t1_v1','dv_t1_v2')"));
    }

    // ---------------------------------------------------------------- SECTION B
    // Tables, views, matviews, sequences and indexes share one name per schema.

    @Test
    void aRelationOfAnyKindOwnsItsNameAgainstEveryOtherKind() throws Exception {
        exec("CREATE TABLE ns_t (a int PRIMARY KEY)");
        exec("CREATE VIEW ns_v AS SELECT a FROM ns_t");
        exec("CREATE SEQUENCE ns_s");
        exec("CREATE INDEX ns_i ON ns_t (a)");
        exec("CREATE MATERIALIZED VIEW ns_m AS SELECT a FROM ns_t");

        assertRejected("42P07", "relation \"ns_v\" already exists", "CREATE TABLE ns_v (x int)");
        assertRejected("42P07", "relation \"ns_s\" already exists", "CREATE TABLE ns_s (x int)");
        assertRejected("42P07", "relation \"ns_i\" already exists", "CREATE TABLE ns_i (x int)");
        assertRejected("42P07", "relation \"ns_m\" already exists", "CREATE TABLE ns_m (x int)");
        assertRejected("42P07", "relation \"ns_t\" already exists", "CREATE VIEW ns_t AS SELECT 1 AS x");
        assertRejected("42P07", "relation \"ns_i\" already exists", "CREATE VIEW ns_i AS SELECT 1 AS x");
        assertRejected("42P07", "relation \"ns_t\" already exists", "CREATE SEQUENCE ns_t");
        assertRejected("42P07", "relation \"ns_v\" already exists", "CREATE SEQUENCE ns_v");
        assertRejected("42P07", "relation \"ns_s\" already exists", "CREATE INDEX ns_s ON ns_t (a)");
        assertRejected("42P07", "relation \"ns_t\" already exists",
                "CREATE MATERIALIZED VIEW ns_t AS SELECT 1 AS x");
    }

    @Test
    void dropNamesAKindAndIfExistsDoesNotExcuseTheWrongOne() throws Exception {
        exec("CREATE TABLE nsd_t (a int PRIMARY KEY)");
        exec("CREATE VIEW nsd_v AS SELECT a FROM nsd_t");
        exec("CREATE SEQUENCE nsd_s");
        exec("CREATE INDEX nsd_i ON nsd_t (a)");
        exec("CREATE MATERIALIZED VIEW nsd_m AS SELECT a FROM nsd_t");

        assertRejected("42809", "\"nsd_i\" is not a table", "DROP TABLE nsd_i");
        assertRejected("42809", "\"nsd_i\" is not a table", "DROP TABLE IF EXISTS nsd_i");
        assertRejected("42809", "\"nsd_v\" is not a table", "DROP TABLE nsd_v");
        assertRejected("42809", "\"nsd_m\" is not a table", "DROP TABLE nsd_m");
        assertRejected("42809", "\"nsd_t\" is not a view", "DROP VIEW nsd_t");
        assertRejected("42809", "\"nsd_i\" is not a view", "DROP VIEW nsd_i");
        assertRejected("42809", "\"nsd_t\" is not a sequence", "DROP SEQUENCE nsd_t");
        assertRejected("42809", "\"nsd_v\" is not a sequence", "DROP SEQUENCE nsd_v");
        assertRejected("42809", "\"nsd_t\" is not an index", "DROP INDEX nsd_t");
    }

    @Test
    void aRenameLandsInTheSameNamespaceAsACreate() throws Exception {
        exec("CREATE TABLE nsr_t (a int PRIMARY KEY)");
        exec("CREATE VIEW nsr_v AS SELECT a FROM nsr_t");
        exec("CREATE SEQUENCE nsr_s");
        assertRejected("42P07", "relation \"nsr_v\" already exists", "ALTER TABLE nsr_t RENAME TO nsr_v");
        assertRejected("42P07", "relation \"nsr_s\" already exists", "ALTER TABLE nsr_t RENAME TO nsr_s");
        assertRejected("42P07", "relation \"nsr_t\" already exists", "ALTER VIEW nsr_v RENAME TO nsr_t");
        assertRejected("42P07", "relation \"nsr_t\" already exists", "ALTER SEQUENCE nsr_s RENAME TO nsr_t");
    }

    @Test
    void theOrdinaryRelationLifecycleKeepsWorking() throws Exception {
        exec("CREATE TABLE nsk_t (a int PRIMARY KEY, b text)");
        assertAccepted("CREATE VIEW nsk_v AS SELECT a FROM nsk_t");
        assertAccepted("CREATE OR REPLACE VIEW nsk_v AS SELECT a FROM nsk_t");
        assertAccepted("CREATE SEQUENCE nsk_s");
        assertAccepted("CREATE INDEX nsk_i ON nsk_t (a)");
        assertAccepted("CREATE UNIQUE INDEX nsk_i2 ON nsk_t (b)");
        assertAccepted("CREATE MATERIALIZED VIEW nsk_m AS SELECT a FROM nsk_t");
        // IF NOT EXISTS means "there is one already, carry on", whatever kind it is
        assertAccepted("CREATE TABLE IF NOT EXISTS nsk_t (zz int)");
        assertAccepted("CREATE TABLE IF NOT EXISTS nsk_v (zz int)");
        assertAccepted("CREATE TABLE IF NOT EXISTS nsk_s (zz int)");
        assertAccepted("CREATE SEQUENCE IF NOT EXISTS nsk_s");
        assertAccepted("CREATE INDEX IF NOT EXISTS nsk_i ON nsk_t (a)");
        // renames onto free names, and back
        assertAccepted("ALTER TABLE nsk_t RENAME TO nsk_t2");
        assertAccepted("ALTER TABLE nsk_t2 RENAME TO nsk_t");
        assertAccepted("ALTER VIEW nsk_v RENAME TO nsk_v2");
        assertAccepted("ALTER VIEW nsk_v2 RENAME TO nsk_v");
        assertAccepted("ALTER SEQUENCE nsk_s RENAME TO nsk_s2");
        assertAccepted("ALTER SEQUENCE nsk_s2 RENAME TO nsk_s");
        // drops of the right kind, and IF EXISTS on names that are simply free
        assertAccepted("DROP INDEX nsk_i2");
        assertAccepted("DROP MATERIALIZED VIEW nsk_m");
        assertAccepted("DROP TABLE IF EXISTS nsk_nosuch");
        assertAccepted("DROP VIEW IF EXISTS nsk_nosuch");
        assertAccepted("DROP SEQUENCE IF EXISTS nsk_nosuch");
        assertAccepted("DROP INDEX IF EXISTS nsk_nosuch");
        assertAccepted("DROP MATERIALIZED VIEW IF EXISTS nsk_nosuch");
        // and the same name is free again in another schema
        assertAccepted("CREATE SCHEMA nsk_s3");
        assertAccepted("CREATE TABLE nsk_s3.nsk_v (id int PRIMARY KEY)");
        assertAccepted("CREATE TABLE nsk_s3.nsk_s (id int PRIMARY KEY)");
    }

    // ---------------------------------------------------------------- SECTION C
    // ALTER on an object that was never created.

    @Test
    void alterOnAnObjectThatWasNeverCreatedIsRefused() {
        assertRejected("42704", "collation \"dz_nosuch\" for encoding \"UTF8\" does not exist",
                "ALTER COLLATION dz_nosuch RENAME TO dz_o");
        assertRejected("42704", "conversion \"dz_nosuch\" does not exist",
                "ALTER CONVERSION dz_nosuch RENAME TO dz_o");
        assertRejected("42704", "language \"dz_nosuch\" does not exist",
                "ALTER LANGUAGE dz_nosuch OWNER TO CURRENT_USER");
        assertRejected("42704", "tablespace \"dz_nosuch\" does not exist",
                "ALTER TABLESPACE dz_nosuch RENAME TO dz_o");
        assertRejected("42883", "function dz_nosuch() does not exist",
                "ALTER FUNCTION dz_nosuch() RENAME TO dz_o");
    }

    @Test
    void alterRuleAndAlterTriggerLookTheObjectUpOnItsRelation() throws Exception {
        exec("CREATE TABLE dz_rel (id int PRIMARY KEY)");
        exec("CREATE FUNCTION dz_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
        assertRejected("42704", "rule \"dz_nosuch\" for relation \"dz_rel\" does not exist",
                "ALTER RULE dz_nosuch ON dz_rel RENAME TO dz_o");
        assertRejected("42704", "trigger \"dz_nosuch\" for table \"dz_rel\" does not exist",
                "ALTER TRIGGER dz_nosuch ON dz_rel RENAME TO dz_o");

        // ... and the rename takes effect on one that is there
        exec("CREATE RULE dz_r1 AS ON INSERT TO dz_rel DO INSTEAD NOTHING");
        assertAccepted("ALTER RULE dz_r1 ON dz_rel RENAME TO dz_r2");
        assertAccepted("DROP RULE dz_r2 ON dz_rel");
        exec("CREATE TRIGGER dz_g1 BEFORE INSERT ON dz_rel FOR EACH ROW EXECUTE FUNCTION dz_tf()");
        assertAccepted("ALTER TRIGGER dz_g1 ON dz_rel RENAME TO dz_g2");
        assertEquals(List.of("dz_g2"), column("SELECT tgname FROM pg_trigger"
                + " WHERE tgrelid = 'dz_rel'::regclass AND NOT tgisinternal ORDER BY tgname"));
    }

    @Test
    void anObjectThatWasCreatedCanBeAlteredAndDropped() {
        assertAccepted("CREATE COLLATION dz_coll (LOCALE = 'C')");
        assertAccepted("ALTER COLLATION dz_coll RENAME TO dz_coll2");
        assertAccepted("ALTER COLLATION dz_coll2 OWNER TO CURRENT_USER");
        assertAccepted("CREATE CONVERSION dz_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1");
        assertAccepted("ALTER CONVERSION dz_conv RENAME TO dz_conv2");
        assertAccepted("CREATE TABLESPACE dz_ts LOCATION '/data/ts'");
        assertAccepted("ALTER TABLESPACE dz_ts RENAME TO dz_ts2");
        assertAccepted("DROP TABLESPACE dz_ts2");
        // the languages PostgreSQL ships with exist without ever being created
        assertAccepted("ALTER LANGUAGE plpgsql OWNER TO CURRENT_USER");
    }

    @Test
    void aTypeRenameCannotTakeANameAnotherTypeAnswersTo() throws Exception {
        exec("CREATE TYPE dz_ta AS ENUM ('x')");
        exec("CREATE TYPE dz_tb AS ENUM ('y')");
        assertRejected("42710", "type \"dz_tb\" already exists", "ALTER TYPE dz_ta RENAME TO dz_tb");
        assertAccepted("ALTER TYPE dz_ta RENAME TO dz_tc");
    }

    // ---------------------------------------------------------------- SECTION D
    // ALTER TYPE attributes, and ALTER DOMAIN against stored rows.

    @Test
    void alterTypeChecksTheAttributeItNames() throws Exception {
        exec("CREATE TYPE dt_ct AS (a int, b text)");
        assertRejected("42701", "column \"a\" of relation \"dt_ct\" already exists",
                "ALTER TYPE dt_ct ADD ATTRIBUTE a int");
        assertRejected("42703", "column \"nosuch\" of relation \"dt_ct\" does not exist",
                "ALTER TYPE dt_ct DROP ATTRIBUTE nosuch");
        assertRejected("42703", "column \"nosuch\" does not exist",
                "ALTER TYPE dt_ct RENAME ATTRIBUTE nosuch TO z");
        // IF EXISTS makes the missing attribute a notice instead
        assertAccepted("ALTER TYPE dt_ct DROP ATTRIBUTE IF EXISTS nosuch");
        assertAccepted("ALTER TYPE dt_ct ADD ATTRIBUTE c int");
        assertAccepted("ALTER TYPE dt_ct RENAME ATTRIBUTE c TO d");
        assertAccepted("ALTER TYPE dt_ct ALTER ATTRIBUTE d TYPE bigint");
        assertAccepted("ALTER TYPE dt_ct DROP ATTRIBUTE d");
    }

    @Test
    void alterDomainLooksAtTheRowsAlreadyStoredUnderIt() throws Exception {
        exec("CREATE DOMAIN dt_dom AS int");
        exec("CREATE TABLE dt_domt (id int PRIMARY KEY, a dt_dom)");
        exec("INSERT INTO dt_domt VALUES (1, NULL)");
        assertRejected("23502", "column \"a\" of table \"dt_domt\" contains null values",
                "ALTER DOMAIN dt_dom SET NOT NULL");
        exec("DELETE FROM dt_domt WHERE id = 1");
        assertAccepted("ALTER DOMAIN dt_dom SET NOT NULL");
        assertAccepted("ALTER DOMAIN dt_dom DROP NOT NULL");

        assertRejected("42704", "constraint \"nosuch\" of domain \"dt_dom\" does not exist",
                "ALTER DOMAIN dt_dom DROP CONSTRAINT nosuch");
        assertAccepted("ALTER DOMAIN dt_dom DROP CONSTRAINT IF EXISTS nosuch");

        exec("INSERT INTO dt_domt VALUES (2, 5)");
        exec("ALTER DOMAIN dt_dom ADD CONSTRAINT dt_dc CHECK (VALUE > 10) NOT VALID");
        // marking it valid is a claim about the rows let through while it was not
        assertRejected("23514",
                "column \"a\" of table \"dt_domt\" contains values that violate the new constraint",
                "ALTER DOMAIN dt_dom VALIDATE CONSTRAINT dt_dc");
        assertRejected("23514",
                "column \"a\" of table \"dt_domt\" contains values that violate the new constraint",
                "ALTER DOMAIN dt_dom ADD CONSTRAINT dt_dc2 CHECK (VALUE > 10)");
        exec("DELETE FROM dt_domt WHERE id = 2");
        assertAccepted("ALTER DOMAIN dt_dom VALIDATE CONSTRAINT dt_dc");
        assertAccepted("ALTER DOMAIN dt_dom DROP CONSTRAINT dt_dc");
        assertAccepted("INSERT INTO dt_domt VALUES (3, 20)");
    }

    // ---------------------------------------------------------------- SECTION E
    // ALTER COLUMN changes that contradict the column.

    @Test
    void alterColumnRefusesChangesTheColumnContradicts() throws Exception {
        exec("CREATE TABLE dc_cc (id int PRIMARY KEY,"
                + " v int GENERATED ALWAYS AS (id * 2) STORED,"
                + " w int, x int GENERATED ALWAYS AS IDENTITY)");
        assertRejected("42P16", "column \"id\" is in a primary key",
                "ALTER TABLE dc_cc ALTER COLUMN id DROP NOT NULL");
        assertRejected("42601", "column \"v\" of relation \"dc_cc\" is a generated column",
                "ALTER TABLE dc_cc ALTER COLUMN v SET DEFAULT 5");
        assertRejected("42601", "column \"v\" of relation \"dc_cc\" is a generated column",
                "ALTER TABLE dc_cc ALTER COLUMN v DROP DEFAULT");
        assertRejected("55000", "is not an identity column",
                "ALTER TABLE dc_cc ALTER COLUMN w DROP IDENTITY");
        assertRejected("3F000", "schema \"dc_nosuchschema\" does not exist",
                "ALTER TABLE dc_cc SET SCHEMA dc_nosuchschema");
    }

    @Test
    void theSameActionsOnTheColumnsTheySuitKeepWorking() throws Exception {
        exec("CREATE TABLE dc_ok (id int PRIMARY KEY,"
                + " v int GENERATED ALWAYS AS (id * 2) STORED,"
                + " w int, x int GENERATED ALWAYS AS IDENTITY)");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN w DROP IDENTITY IF EXISTS");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN w SET DEFAULT 5");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN w DROP DEFAULT");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN w SET NOT NULL");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN w DROP NOT NULL");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN x DROP IDENTITY");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN x DROP NOT NULL");
        exec("INSERT INTO dc_ok (id, w) VALUES (1, 7)");
        assertEquals("2", scalar("SELECT v::text FROM dc_ok WHERE id = 1"));
        // once the key is gone the column may be made nullable
        assertAccepted("ALTER TABLE dc_ok DROP CONSTRAINT dc_ok_pkey");
        assertAccepted("ALTER TABLE dc_ok ALTER COLUMN id DROP NOT NULL");
    }

    // ---------------------------------------------------------------- SECTION F
    // A multi-action ALTER TABLE is one statement, not a script.

    @Test
    void aConstraintMayReadAColumnTheSameStatementAdds() throws Exception {
        exec("CREATE TABLE ma_t (id int PRIMARY KEY, a int)");
        exec("INSERT INTO ma_t VALUES (1, 1)");
        assertAccepted("ALTER TABLE ma_t ADD CONSTRAINT ma_ck CHECK (b > 0),"
                + " ADD COLUMN b int NOT NULL DEFAULT 5");
        assertEquals("5", scalar("SELECT b::text FROM ma_t WHERE id = 1"));
    }

    @Test
    void everythingDroppedIsDroppedBeforeAnythingIsAdded() throws Exception {
        exec("CREATE TABLE ma_d (id int PRIMARY KEY, b int)");
        // the drop pass runs first, so it looks for a column that is not there yet
        assertRejected("42703", "column \"c\" of relation \"ma_d\" does not exist",
                "ALTER TABLE ma_d ADD COLUMN c text DEFAULT 'z', DROP COLUMN c");
        // ... and the same two actions the other way round replace the column
        assertAccepted("ALTER TABLE ma_d DROP COLUMN b, ADD COLUMN b int DEFAULT 9");
        assertEquals(List.of("b", "id"), column("SELECT column_name FROM information_schema.columns"
                + " WHERE table_name = 'ma_d' ORDER BY column_name"));
    }

    @Test
    void neighbouringMultiActionShapesKeepWorking() throws Exception {
        exec("CREATE TABLE ma_n (id int PRIMARY KEY, a int)");
        exec("INSERT INTO ma_n VALUES (1, 1)");
        assertAccepted("ALTER TABLE ma_n ADD COLUMN b int, ADD COLUMN c int");
        assertAccepted("ALTER TABLE ma_n ADD COLUMN d int DEFAULT 4, ALTER COLUMN d SET NOT NULL");
        assertAccepted("ALTER TABLE ma_n DROP COLUMN c, DROP COLUMN b");
        assertAccepted("ALTER TABLE ma_n ALTER COLUMN a TYPE bigint, ADD COLUMN e bigint");
        assertAccepted("ALTER TABLE ma_n ADD COLUMN f int NOT NULL DEFAULT 6,"
                + " ADD CONSTRAINT ma_n_ck CHECK (f > 0)");
        assertAccepted("ALTER TABLE ma_n DROP CONSTRAINT ma_n_ck,"
                + " ADD CONSTRAINT ma_n_ck CHECK (f >= 0)");
        assertEquals(List.of("a", "d", "e", "f", "id"),
                column("SELECT column_name FROM information_schema.columns"
                        + " WHERE table_name = 'ma_n' ORDER BY column_name"));
    }

    // ---------------------------------------------------------------- SECTION G
    // A child inherits one column, so its parents have to agree about it.

    @Test
    void parentsThatDisagreeAboutAColumnCannotBeInheritedTogether() throws Exception {
        exec("CREATE TABLE ih_p1 (shared int, x int)");
        exec("CREATE TABLE ih_p2 (shared bigint, y int)");
        assertRejected("42804", "inherited column \"shared\" has a type conflict",
                "CREATE TABLE ih_c1 () INHERITS (ih_p1, ih_p2)");

        exec("CREATE TABLE ih_p3 (tomorrow date DEFAULT '2001-01-01')");
        exec("CREATE TABLE ih_p4 (tomorrow date DEFAULT '2002-02-02')");
        assertRejected("42611", "column \"tomorrow\" inherits conflicting default values",
                "CREATE TABLE ih_c2 () INHERITS (ih_p3, ih_p4)");

        assertRejected("42804", "column \"shared\" has a type conflict",
                "CREATE TABLE ih_c3 (shared bigint) INHERITS (ih_p1)");
        assertRejected("42P07", "would be inherited from more than once",
                "CREATE TABLE ih_c9 () INHERITS (ih_p1, ih_p1)");
    }

    @Test
    void parentsThatAgreeStillMergeTheColumn() throws Exception {
        exec("CREATE TABLE ig_p1 (shared int, x int, tomorrow date DEFAULT '2001-01-01')");
        exec("CREATE TABLE ig_p2 (shared int, z int, tomorrow date DEFAULT '2001-01-01')");
        assertAccepted("CREATE TABLE ig_c1 () INHERITS (ig_p1, ig_p2)");
        assertAccepted("CREATE TABLE ig_c2 (shared int) INHERITS (ig_p1)");
        assertAccepted("CREATE TABLE ig_c3 () INHERITS (ig_c1)");
        assertEquals(List.of("shared", "tomorrow", "x", "z"),
                column("SELECT column_name FROM information_schema.columns"
                        + " WHERE table_name = 'ig_c1' ORDER BY column_name"));
        exec("INSERT INTO ig_c1 (shared, x, z) VALUES (1, 2, 3)");
        assertEquals("1", scalar("SELECT shared::text FROM ig_p1 ORDER BY shared"));
    }

    @Test
    void aNotNullDeclaredOnTheParentReachesTheChildsRows() throws Exception {
        exec("CREATE TABLE ih_p6 (id int PRIMARY KEY, a int NOT NULL)");
        exec("CREATE TABLE ih_c6 () INHERITS (ih_p6)");
        assertRejected("23502",
                "null value in column \"a\" of relation \"ih_c6\" violates not-null constraint",
                "INSERT INTO ih_c6 (id, a) VALUES (1, NULL)");
    }

    // ---------------------------------------------------------------- SECTION H
    // A partition key must answer the same way every time it is asked.

    @Test
    void aPartitionKeyExpressionThatMayChangeItsAnswerIsRefused() {
        assertRejected("42P17", "functions in partition key expression must be marked IMMUTABLE",
                "CREATE TABLE pk_1 (i int) PARTITION BY RANGE ((random()))");
        assertRejected("42P17", "functions in partition key expression must be marked IMMUTABLE",
                "CREATE TABLE pk_2 (i timestamptz) PARTITION BY RANGE ((now()))");
        assertRejected("42P17", "functions in partition key expression must be marked IMMUTABLE",
                "CREATE TABLE pk_3 (i int) PARTITION BY RANGE ((i + random()::int))");
        // a cast off a timestamptz reads the session time zone, so it is stable, not immutable
        assertRejected("42P17", "functions in partition key expression must be marked IMMUTABLE",
                "CREATE TABLE pk_4 (t timestamptz) PARTITION BY RANGE ((t::date))");
    }

    @Test
    void immutableExpressionPartitionKeysKeepBeingAccepted() throws Exception {
        assertAccepted("CREATE TABLE pk_ok1 (i int) PARTITION BY RANGE ((i * 2))");
        assertAccepted("CREATE TABLE pk_ok2 (i int) PARTITION BY LIST ((i % 4))");
        assertAccepted("CREATE TABLE pk_ok3 (i int) PARTITION BY RANGE ((abs(i)))");
        assertAccepted("CREATE TABLE pk_ok4 (t text) PARTITION BY RANGE ((lower(t)))");
        assertAccepted("CREATE TABLE pk_ok5 (i int) PARTITION BY HASH ((i + 1))");
        assertAccepted("CREATE TABLE pk_ok6 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE pk_ok6_a PARTITION OF pk_ok6 FOR VALUES FROM (0) TO (10)");
        exec("INSERT INTO pk_ok6 VALUES (1), (2)");
        assertEquals("2", scalar("SELECT count(*)::text FROM pk_ok6"));
    }

    // ---------------------------------------------------------------- SECTION I
    // ALTER TABLE ALTER CONSTRAINT knows which kind it is looking at.

    @Test
    void alterConstraintOptionsAreCheckedAgainstTheConstraintsKind() throws Exception {
        exec("CREATE TABLE ac_p (id int PRIMARY KEY)");
        exec("CREATE TABLE ac_t (id int PRIMARY KEY, p int, q int,"
                + " CONSTRAINT ac_fk FOREIGN KEY (p) REFERENCES ac_p(id))");
        assertRejected("42601", "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
                "ALTER TABLE ac_t ALTER CONSTRAINT ac_fk NOT DEFERRABLE INITIALLY DEFERRED");
        assertRejected("42809", "constraint \"ac_fk\" of relation \"ac_t\" is not a not-null constraint",
                "ALTER TABLE ac_t ALTER CONSTRAINT ac_fk NO INHERIT");
        assertRejected("0A000", "constraints cannot be altered to be NOT VALID",
                "ALTER TABLE ac_t ALTER CONSTRAINT ac_fk NOT VALID");
        assertRejected("42704", "constraint \"ac_nosuch\" of relation \"ac_t\" does not exist",
                "ALTER TABLE ac_t ALTER CONSTRAINT ac_nosuch NOT DEFERRABLE");
        assertAccepted("ALTER TABLE ac_t ALTER CONSTRAINT ac_fk DEFERRABLE INITIALLY DEFERRED");
        assertAccepted("ALTER TABLE ac_t ALTER CONSTRAINT ac_fk NOT DEFERRABLE");
    }

    @Test
    void aNamedNotNullConstraintIsTheKindInheritIsFor() throws Exception {
        exec("CREATE TABLE ac_nn (id int PRIMARY KEY, q int)");
        exec("ALTER TABLE ac_nn ADD CONSTRAINT ac_nn_q NOT NULL q");
        assertAccepted("ALTER TABLE ac_nn ALTER CONSTRAINT ac_nn_q NO INHERIT");
        assertAccepted("ALTER TABLE ac_nn ALTER CONSTRAINT ac_nn_q INHERIT");
        assertRejected("23502",
                "null value in column \"q\" of relation \"ac_nn\" violates not-null constraint",
                "INSERT INTO ac_nn (id, q) VALUES (1, NULL)");
        // dropping the constraint is what makes the column nullable again
        assertAccepted("ALTER TABLE ac_nn DROP CONSTRAINT ac_nn_q");
        assertAccepted("INSERT INTO ac_nn (id, q) VALUES (2, NULL)");
        // adding it over rows that already hold a null cannot work
        assertRejected("23502", "column \"q\" of relation \"ac_nn\" contains null values",
                "ALTER TABLE ac_nn ADD CONSTRAINT ac_nn_q2 NOT NULL q");
    }

    @Test
    void aCheckConstraintIsNeitherOfThoseKinds() throws Exception {
        exec("CREATE TABLE ac_ck (id int PRIMARY KEY, q int)");
        exec("ALTER TABLE ac_ck ADD CONSTRAINT ac_ck_c CHECK (q IS NULL OR q > 0)");
        assertRejected("42809", "constraint \"ac_ck_c\" of relation \"ac_ck\" is not a not-null constraint",
                "ALTER TABLE ac_ck ALTER CONSTRAINT ac_ck_c NO INHERIT");
        assertRejected("42809", "constraint \"ac_ck_c\" of relation \"ac_ck\" is not a foreign key constraint",
                "ALTER TABLE ac_ck ALTER CONSTRAINT ac_ck_c DEFERRABLE");
    }

    // ---------------------------------------------------------------- SECTION J
    // CREATE TRIGGER, and errors raised inside a WHEN condition.

    @Test
    void anInsteadOfTriggerCannotHaveAColumnList() throws Exception {
        exec("CREATE TABLE tg_t (id int PRIMARY KEY, v int, d int)");
        exec("CREATE VIEW tg_v AS SELECT id, v FROM tg_t");
        exec("CREATE FUNCTION tg_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
        assertRejected("0A000", "INSTEAD OF triggers cannot have column lists",
                "CREATE TRIGGER tg_g8 INSTEAD OF UPDATE OF id ON tg_v FOR EACH ROW"
                        + " EXECUTE FUNCTION tg_f()");
        assertAccepted("CREATE TRIGGER tg_ok INSTEAD OF UPDATE ON tg_v FOR EACH ROW"
                + " EXECUTE FUNCTION tg_f()");
    }

    @Test
    void aTriggerNameIsOnlyUniqueWithinItsRelation() throws Exception {
        exec("CREATE TABLE tg_a (id int PRIMARY KEY)");
        exec("CREATE TABLE tg_b (id int PRIMARY KEY)");
        exec("CREATE FUNCTION tg_f2() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tg_same BEFORE INSERT ON tg_a FOR EACH ROW EXECUTE FUNCTION tg_f2()");
        assertRejected("42710", "trigger \"tg_same\" for relation \"tg_a\" already exists",
                "CREATE TRIGGER tg_same AFTER UPDATE ON tg_a FOR EACH ROW EXECUTE FUNCTION tg_f2()");
        assertAccepted("CREATE TRIGGER tg_same BEFORE INSERT ON tg_b FOR EACH ROW"
                + " EXECUTE FUNCTION tg_f2()");
        // both are visible, each against its own relation
        assertEquals(List.of("tg_same"), column("SELECT tgname FROM pg_trigger"
                + " WHERE tgrelid = 'tg_a'::regclass AND NOT tgisinternal ORDER BY tgname"));
        assertEquals(List.of("tg_same"), column("SELECT tgname FROM pg_trigger"
                + " WHERE tgrelid = 'tg_b'::regclass AND NOT tgisinternal ORDER BY tgname"));
    }

    @Test
    void anErrorInsideAWhenConditionBelongsToTheStatement() throws Exception {
        exec("CREATE TABLE tg_w (id int PRIMARY KEY, v int, d int)");
        exec("CREATE FUNCTION tg_f3() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tg_wg BEFORE INSERT ON tg_w FOR EACH ROW WHEN (1/NEW.d > 0)"
                + " EXECUTE FUNCTION tg_f3()");
        assertRejected("22012", "division by zero", "INSERT INTO tg_w VALUES (1, 1, 0)");
        assertAccepted("INSERT INTO tg_w VALUES (2, 1, 1)");
        assertEquals("1", scalar("SELECT count(*)::text FROM tg_w"));
        // a WHEN over a NULL is unknown, not an error, and simply does not fire
        exec("CREATE TABLE tg_n (id int PRIMARY KEY, d int)");
        exec("CREATE TRIGGER tg_ng BEFORE INSERT ON tg_n FOR EACH ROW WHEN (NEW.d > 0)"
                + " EXECUTE FUNCTION tg_f3()");
        assertAccepted("INSERT INTO tg_n VALUES (1, NULL)");
    }

    // ---------------------------------------------------------------- SECTION K
    // REFRESH MATERIALIZED VIEW CONCURRENTLY prerequisites.

    @Test
    void refreshConcurrentlyStatesItsPrerequisites() throws Exception {
        exec("CREATE TABLE mv_t (id int PRIMARY KEY, v int)");
        exec("INSERT INTO mv_t VALUES (1,1),(2,2)");
        exec("CREATE MATERIALIZED VIEW mv_m AS SELECT id, v FROM mv_t");
        assertRejected("55000", "cannot refresh materialized view \"public.mv_m\" concurrently",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_m");
        assertRejected("42601", "CONCURRENTLY and WITH NO DATA options cannot be used together",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_m WITH NO DATA");
        // a partial index does not cover every row, so it cannot identify them all
        exec("CREATE UNIQUE INDEX mv_m_partial ON mv_m (id) WHERE v > 1");
        assertRejected("55000", "cannot refresh materialized view \"public.mv_m\" concurrently",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_m");
        assertRejected("0A000", "\"mv_t\" is not a materialized view",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_t");
        assertRejected("42P01", "relation \"mv_nosuch\" does not exist",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_nosuch");
        // a full unique index makes it work
        exec("CREATE UNIQUE INDEX mv_m_ui ON mv_m (id)");
        assertAccepted("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_m");
        assertEquals("2", scalar("SELECT count(*)::text FROM mv_m"));
        assertAccepted("REFRESH MATERIALIZED VIEW mv_m");
        assertEquals("2", scalar("SELECT count(*)::text FROM mv_m"));
    }

    // ---------------------------------------------------------------- SECTION L
    // PERMISSIVE policies are OR-ed; RESTRICTIVE ones are AND-ed.

    @Test
    void permissivePoliciesAreOredOnTheWritePath() throws Exception {
        exec("CREATE ROLE rls_writer LOGIN");
        exec("CREATE TABLE rls_w (id int PRIMARY KEY, s int)");
        exec("INSERT INTO rls_w VALUES (1, 1), (2, 200)");
        exec("ALTER TABLE rls_w ENABLE ROW LEVEL SECURITY");
        exec("GRANT SELECT, INSERT, UPDATE, DELETE ON rls_w TO rls_writer");
        exec("CREATE POLICY rls_wa ON rls_w AS PERMISSIVE FOR INSERT WITH CHECK (id > 0)");
        exec("CREATE POLICY rls_wb ON rls_w AS PERMISSIVE FOR INSERT WITH CHECK (id > 100)");
        exec("SET ROLE rls_writer");
        try {
            // a second permissive policy widens what may be written; 5 satisfies the first one
            assertAccepted("INSERT INTO rls_w VALUES (5, 5)");
            assertAccepted("INSERT INTO rls_w VALUES (500, 5)");
            assertRejected("42501", "new row violates row-level security policy for table \"rls_w\"",
                    "INSERT INTO rls_w VALUES (-5, 5)");
        } finally {
            exec("RESET ROLE");
        }
        assertEquals(List.of("1", "2", "5", "500"),
                column("SELECT id::text FROM rls_w ORDER BY id"));
    }

    @Test
    void restrictivePoliciesAreAndedAndTheOneThatRefusedIsNamed() throws Exception {
        exec("CREATE ROLE rls_res LOGIN");
        exec("CREATE TABLE rls_r (id int PRIMARY KEY, s int)");
        exec("ALTER TABLE rls_r ENABLE ROW LEVEL SECURITY");
        exec("GRANT SELECT, INSERT ON rls_r TO rls_res");
        exec("CREATE POLICY rls_ra ON rls_r AS PERMISSIVE FOR INSERT WITH CHECK (id > 0)");
        exec("CREATE POLICY rls_rc ON rls_r AS RESTRICTIVE FOR INSERT WITH CHECK (id < 1000)");
        exec("SET ROLE rls_res");
        try {
            assertAccepted("INSERT INTO rls_r VALUES (6, 6)");
            assertRejected("42501",
                    "new row violates row-level security policy \"rls_rc\" for table \"rls_r\"",
                    "INSERT INTO rls_r VALUES (2000, 5)");
        } finally {
            exec("RESET ROLE");
        }
    }

    @Test
    void permissiveUpdatePoliciesAreOredOnBothHalves() throws Exception {
        exec("CREATE ROLE rls_upd LOGIN");
        exec("CREATE TABLE rls_u (id int PRIMARY KEY, s int)");
        exec("INSERT INTO rls_u VALUES (1, 1), (6, 6)");
        exec("ALTER TABLE rls_u ENABLE ROW LEVEL SECURITY");
        exec("GRANT SELECT, UPDATE ON rls_u TO rls_upd");
        exec("CREATE POLICY rls_us ON rls_u AS PERMISSIVE FOR SELECT USING (true)");
        exec("CREATE POLICY rls_u1 ON rls_u AS PERMISSIVE FOR UPDATE USING (id = 1) WITH CHECK (s = 10)");
        exec("CREATE POLICY rls_u2 ON rls_u AS PERMISSIVE FOR UPDATE USING (id = 6) WITH CHECK (s = 20)");
        exec("SET ROLE rls_upd");
        try {
            // USING lets either row through, WITH CHECK lets either value through
            assertAccepted("UPDATE rls_u SET s = 10 WHERE id = 6");
            assertAccepted("UPDATE rls_u SET s = 20 WHERE id = 1");
            assertRejected("42501", "new row violates row-level security policy for table \"rls_u\"",
                    "UPDATE rls_u SET s = 30 WHERE id = 1");
        } finally {
            exec("RESET ROLE");
        }
        assertEquals(List.of("20", "10"), column("SELECT s::text FROM rls_u ORDER BY id"));
    }

    @Test
    void aTableWithRowLevelSecurityAndNoPolicyStillDeniesEveryone() throws Exception {
        exec("CREATE ROLE rls_none LOGIN");
        exec("CREATE TABLE rls_n (id int PRIMARY KEY)");
        exec("INSERT INTO rls_n VALUES (1)");
        exec("ALTER TABLE rls_n ENABLE ROW LEVEL SECURITY");
        exec("GRANT SELECT, INSERT ON rls_n TO rls_none");
        exec("SET ROLE rls_none");
        try {
            assertRejected("42501", "new row violates row-level security policy for table \"rls_n\"",
                    "INSERT INTO rls_n VALUES (2)");
            assertEquals("0", scalar("SELECT count(*)::text FROM rls_n"));
        } finally {
            exec("RESET ROLE");
        }
    }
}
