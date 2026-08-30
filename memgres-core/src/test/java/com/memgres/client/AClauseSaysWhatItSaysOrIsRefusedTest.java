package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A clause says what it says, or it is refused where it stops making sense.
 *
 * <p>Several clauses had a part that could be left out and quietly stood for something: a
 * SET CONSTRAINTS with no mode meant IMMEDIATE, which is the opposite of what a statement written
 * to defer them asks for; a CASE with no WHEN chose between nothing and answered null; a DROP ROLE
 * naming three roles dropped one and reported success. Others were refused where PostgreSQL
 * accepts: a storage parameter belonging to the TOAST table, and one written as a bare flag.
 *
 * <p>And a clause that is wrong is wrong at the word it is wrong at. A window frame's EXCLUDE
 * threw a bare exception, which reached the client as an internal error rather than as the syntax
 * error it is.
 */
class AClauseSaysWhatItSaysOrIsRefusedTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
            }
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    /** SET CONSTRAINTS says which way to set them, and with nothing written it asks for nothing. */
    @Test
    void setConstraintsSaysWhichWay() {
        assertEquals("42601", stateOf("SET CONSTRAINTS ALL"));
        assertNull(stateOf("SET CONSTRAINTS ALL IMMEDIATE"));
        assertNull(stateOf("SET CONSTRAINTS ALL DEFERRED"));
        assertEquals("42704", stateOf("SET CONSTRAINTS zac_no_such_constraint DEFERRED"));
        assertNull(stateOf("SET CONSTRAINTS ALL IMMEDIATE"));
    }

    /** A CASE chooses between its WHEN branches, and with none there is nothing to choose. */
    @Test
    void aCaseHasAtLeastOneWhen() {
        assertEquals("42601", stateOf("SELECT CASE 1 END"));
        assertEquals("42601", stateOf("SELECT CASE 1 ELSE 2 END"));
        assertEquals("42601", stateOf("SELECT CASE END"));
    }

    /** What may follow EXCLUDE is four spellings, and anything else is a syntax error. */
    @Test
    void aWindowFramesExcludeIsOneOfFourSpellings() {
        assertEquals("42601", stateOf("SELECT sum(g) OVER (ORDER BY g ROWS BETWEEN"
                + " UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE BOGUS) FROM generate_series(1,3) g"));
        assertEquals("42601", stateOf("SELECT sum(g) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING"
                + " EXCLUDE) FROM generate_series(1,3) g"));
        // The four that are spellings still work.
        assertNull(stateOf("SELECT sum(g) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING"
                + " EXCLUDE CURRENT ROW) FROM generate_series(1,3) g"));
        assertNull(stateOf("SELECT sum(g) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING"
                + " EXCLUDE NO OTHERS) FROM generate_series(1,3) g"));
    }

    /** DROP ROLE drops every role it names. */
    @Test
    void dropRoleDropsEveryRoleItNames() throws SQLException {
        exec("CREATE ROLE zcl_ra NOLOGIN");
        exec("CREATE ROLE zcl_rb NOLOGIN");
        exec("CREATE ROLE zcl_rc NOLOGIN");
        exec("DROP ROLE zcl_ra, zcl_rb, zcl_rc");
        assertEquals(List.of(), rows("SELECT rolname::text FROM pg_roles"
                + " WHERE rolname IN ('zcl_ra','zcl_rb','zcl_rc') ORDER BY 1"));
    }

    /** A storage parameter may belong to the TOAST table, and may be written as a bare flag. */
    @Test
    void aStorageParameterMayBeNamespacedOrValueless() throws SQLException {
        try {
            exec("CREATE TABLE zcl_o2 (a int, s text) WITH (toast.autovacuum_enabled = true)");
            exec("CREATE TABLE zcl_w4 (a int) WITH (autovacuum_enabled)");
            // A flag with no value is on, which is what the relation reports.
            assertEquals("{autovacuum_enabled=true}", one("SELECT reloptions::text FROM pg_class"
                    + " WHERE relname='zcl_w4'"));
            // A TOAST parameter is set on the TOAST table, so it is not the relation's own.
            assertEquals("null", one("SELECT reloptions::text FROM pg_class"
                    + " WHERE relname='zcl_o2'"));
            // A TOAST table takes only the parameters that govern when it is maintained.
            assertEquals("22023", stateOf("CREATE TABLE zcl_o1 (a int)"
                    + " WITH (toast.nosuchoption = 1)"));
            assertTrue(messageOf("CREATE TABLE zcl_o3 (a int, s text)"
                    + " WITH (toast.fillfactor = 50)")
                    .contains("unrecognized parameter \"fillfactor\""));
            // A count is a count, so a flag standing where one belongs is not a value for it.
            assertTrue(messageOf("CREATE TABLE zcl_f (a int) WITH (fillfactor)")
                    .contains("invalid value for integer option \"fillfactor\": true"));
        } finally {
            exec("DROP TABLE IF EXISTS zcl_o2, zcl_w4 CASCADE");
        }
    }

    /** Only what carries a collation may be indexed under one. */
    @Test
    void collateOnAnIndexKeyIsCheckedAgainstWhatTheKeyIs() throws SQLException {
        exec("CREATE TABLE zcl_ix (arr int[], sarr text[])");
        exec("CREATE DOMAIN zcl_di AS int");
        exec("CREATE DOMAIN zcl_dt AS text");
        exec("CREATE TABLE zcl_d (a zcl_di, b zcl_dt)");
        exec("CREATE TYPE zcl_en AS ENUM ('a','b')");
        exec("CREATE TYPE zcl_co AS (x int)");
        exec("CREATE TABLE zcl_c (e zcl_en, k zcl_co)");
        try {
            assertEquals("42804", stateOf("CREATE INDEX zcl_i1 ON zcl_ix (arr COLLATE \"C\")"));
            assertTrue(messageOf("CREATE INDEX zcl_i1 ON zcl_ix (arr COLLATE \"C\")")
                    .contains("collations are not supported by type integer[]"));
            assertTrue(messageOf("CREATE INDEX zcl_i3 ON zcl_d (a COLLATE \"C\")")
                    .contains("collations are not supported by type public.zcl_di"));
            assertTrue(messageOf("CREATE INDEX zcl_i5 ON zcl_c (e COLLATE \"C\")")
                    .contains("collations are not supported by type public.zcl_en"));
            assertTrue(messageOf("CREATE INDEX zcl_i6 ON zcl_c (k COLLATE \"C\")")
                    .contains("collations are not supported by type public.zcl_co"));
            // An array of a string type carries the collation of what it holds, and a domain
            // over one carries the collation it is over.
            assertNull(stateOf("CREATE INDEX zcl_i2 ON zcl_ix (sarr COLLATE \"C\")"));
            assertNull(stateOf("CREATE INDEX zcl_i4 ON zcl_d (b COLLATE \"C\")"));
        } finally {
            exec("DROP TABLE zcl_ix, zcl_d, zcl_c CASCADE");
            exec("DROP DOMAIN zcl_di, zcl_dt");
            exec("DROP TYPE zcl_en, zcl_co");
        }
    }

    /** A field of a composite is a value of the type that field was declared with. */
    @Test
    void aCompositeFieldIsDescribedByTheTypeItWasDeclaredWith() throws SQLException {
        exec("CREATE TYPE zcl_ct AS (b boolean, s text, n int, d date)");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT ('(t,hi,1,2020-01-01)'::zcl_ct).b AS b,"
                     + " ('(t,hi,1,2020-01-01)'::zcl_ct).n AS n,"
                     + " ('(t,hi,1,2020-01-01)'::zcl_ct).d AS d")) {
            assertEquals("bool", rs.getMetaData().getColumnTypeName(1));
            assertEquals("int4", rs.getMetaData().getColumnTypeName(2));
            assertEquals("date", rs.getMetaData().getColumnTypeName(3));
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        } finally {
            exec("DROP TYPE zcl_ct");
        }
    }

    /** A zone written as a displacement counts less than a week either way. */
    @Test
    void aFixedZoneOffsetIsOneTheZoneDatabaseCanHold() throws SQLException {
        try {
            assertEquals("22023", stateOf("SET TIME ZONE 168"));
            assertTrue(messageOf("SET TIME ZONE 123456")
                    .contains("invalid value for parameter \"TimeZone\": \"123456\""));
            assertEquals("22023", stateOf("SET TIME ZONE -168"));
            exec("SET TIME ZONE 167");
            assertEquals("<+167>-167", one("SHOW TimeZone"));
            exec("SET TIME ZONE 7");
            assertEquals("<+07>-07", one("SHOW TimeZone"));
        } finally {
            exec("SET TIME ZONE 'UTC'");
        }
    }

    /** SET ROLE names the role the same way whichever of its three spellings is written. */
    @Test
    void setRoleNamesTheRoleWhicheverSpellingIsWritten() throws SQLException {
        exec("CREATE ROLE zcl_role NOLOGIN");
        try {
            exec("SET SESSION ROLE zcl_role");
            assertEquals("zcl_role", one("SELECT current_role"));
            exec("SET SESSION ROLE NONE");
            exec("SET ROLE zcl_role");
            assertEquals("zcl_role", one("SELECT current_role"));
            exec("RESET ROLE");
            conn.setAutoCommit(false);
            exec("SET LOCAL ROLE zcl_role");
            assertEquals("zcl_role", one("SELECT current_role"));
            conn.rollback();
            conn.setAutoCommit(true);
        } finally {
            conn.setAutoCommit(true);
            exec("RESET ROLE");
            exec("DROP ROLE zcl_role");
        }
    }

    /**
     * A named NOT NULL over a column that already refuses a null creates nothing, and is folded
     * into the constraint already there rather than refused.
     */
    @Test
    void asecondNamedNotNullIsFoldedIntoTheOneAlreadyThere() throws SQLException {
        exec("CREATE TABLE zcl_n (i int NOT NULL)");
        try {
            exec("ALTER TABLE zcl_n ADD CONSTRAINT zcl_n2 NOT NULL i");
            exec("ALTER TABLE ONLY zcl_n ADD CONSTRAINT zcl_n3 NOT NULL i");
            exec("ALTER TABLE zcl_n ADD CONSTRAINT zcl_n4 NOT NULL i NOT VALID");
            // The constraint already there keeps its name and stays as validated as it was, so
            // the names the later statements were written with are the names of nothing.
            assertEquals(List.of("zcl_n_i_not_null/true"),
                    rows("SELECT conname::text || '/' || convalidated::text FROM pg_constraint"
                            + " WHERE conrelid='zcl_n'::regclass AND contype='n'"));
            assertEquals("42704", stateOf("ALTER TABLE zcl_n DROP CONSTRAINT zcl_n2"));
            // A declaration that contradicts the one already there is refused, and says which.
            assertTrue(messageOf("ALTER TABLE zcl_n ADD CONSTRAINT zcl_n5 NOT NULL i NO INHERIT")
                    .contains("cannot change NO INHERIT status of NOT NULL constraint"
                            + " \"zcl_n_i_not_null\" on relation \"zcl_n\""));
        } finally {
            exec("DROP TABLE zcl_n");
        }
    }

    /** A NOT NULL declared NO INHERIT stops where it was declared, and says so. */
    @Test
    void aNotNullDeclaredNoInheritIsRecordedAsStoppingThere() throws SQLException {
        exec("CREATE TABLE zcl_ni (i int)");
        try {
            exec("ALTER TABLE zcl_ni ADD CONSTRAINT zcl_nin NOT NULL i NO INHERIT");
            assertEquals("t", one("SELECT connoinherit FROM pg_constraint"
                    + " WHERE conrelid='zcl_ni'::regclass AND contype='n'"));
            // A partition always carries the partitioned table's rules, so there is no such
            // constraint to declare there.
            assertTrue(messageOf("CREATE TABLE zcl_np (i int NOT NULL NO INHERIT)"
                    + " PARTITION BY RANGE (i)")
                    .contains("not-null constraints on partitioned tables cannot be NO INHERIT"));
        } finally {
            exec("DROP TABLE zcl_ni");
        }
    }

    /** A rule is rewritten before a generated column is computed, so NEW reads null for one. */
    @Test
    void aRulesNewReadsNullForAColumnTheSystemComputes() throws SQLException {
        exec("CREATE TABLE zcl_r (a int, s int GENERATED ALWAYS AS (a*2) STORED,"
                + " v int GENERATED ALWAYS AS (a*3) VIRTUAL, t text DEFAULT 'd')");
        exec("CREATE TABLE zcl_rlog (m text)");
        exec("CREATE RULE zcl_rr AS ON INSERT TO zcl_r DO ALSO INSERT INTO zcl_rlog"
                + " VALUES (coalesce(NEW.s::text,'null') || '/' || coalesce(NEW.v::text,'null')"
                + " || '/' || coalesce(NEW.t,'null'))");
        try {
            exec("INSERT INTO zcl_r (a) VALUES (4)");
            // The default of a column the statement left out does reach NEW; a generated column
            // does not, because there is nothing in the target list to put in its place.
            assertEquals("null/null/d", one("SELECT m FROM zcl_rlog"));
            // The row itself still gets the computed values.
            assertEquals("4/8/12", one("SELECT a || '/' || s || '/' || v FROM zcl_r"));
        } finally {
            exec("DROP TABLE zcl_r, zcl_rlog CASCADE");
        }
    }

    /** The three that name a role, a database or a schema are named with the identifier type. */
    @Test
    void theValueFunctionsAnswerWithTheTypeIdentifiersAreKeptIn() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT current_role, current_user, session_user,"
                     + " user, current_catalog, current_schema")) {
            ResultSetMetaData md = rs.getMetaData();
            for (int c = 1; c <= md.getColumnCount(); c++) {
                assertEquals("name", md.getColumnTypeName(c), md.getColumnLabel(c));
            }
            // A column takes the name of the function that filled it, and USER is written USER.
            assertEquals("user", md.getColumnLabel(4));
        }
    }
}
