package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A comment names one object of one kind, and says so where it does not.
 *
 * <p>What COMMENT names is written the way that kind of object is written: a cast by the two types
 * it converts between, an operator by its spelling and its operands, an operator class by its
 * access method, a trigger by its relation. Read instead as "every word before IS, the last of
 * them the name", a kind nobody defined was a kind, a comma-separated list was one target, and a
 * cast's parentheses were thrown away along with the types inside them.
 *
 * <p>Once the name is read it has to reach something. The relation kinds share one namespace, so a
 * name another kind of relation holds is that other kind rather than nothing at all. And a comment
 * is part of the definition of what it describes, so it is written inside the transaction that
 * wrote it and goes with it.
 */
class ACommentNamesOneObjectOfOneKindTest {

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

    /** The relation kinds share a namespace, so the wrong kind is that kind and not nothing. */
    @Test
    void aRelationOfTheWrongKindIsSaidToBeTheKindItIs() throws SQLException {
        exec("CREATE TABLE zac_t (id int, v text)");
        exec("CREATE VIEW zac_vw AS SELECT id FROM zac_t");
        exec("CREATE SEQUENCE zac_sq");
        exec("CREATE INDEX zac_ix ON zac_t (id)");
        try {
            assertEquals("42809", stateOf("COMMENT ON VIEW zac_t IS 'x'"));
            assertTrue(messageOf("COMMENT ON VIEW zac_t IS 'x'").contains("\"zac_t\" is not a view"));
            assertTrue(messageOf("COMMENT ON INDEX zac_t IS 'x'").contains("is not an index"));
            assertTrue(messageOf("COMMENT ON SEQUENCE zac_t IS 'x'").contains("is not a sequence"));
            assertTrue(messageOf("COMMENT ON MATERIALIZED VIEW zac_t IS 'x'")
                    .contains("is not a materialized view"));
            assertTrue(messageOf("COMMENT ON TABLE zac_vw IS 'x'").contains("is not a table"));
            assertTrue(messageOf("COMMENT ON TABLE zac_sq IS 'x'").contains("is not a table"));
            assertTrue(messageOf("COMMENT ON TABLE zac_ix IS 'x'").contains("is not a table"));
            // A name no relation holds is the relation that is not there.
            assertEquals("42P01", stateOf("COMMENT ON VIEW zac_nosuchrel IS 'x'"));
            assertTrue(messageOf("COMMENT ON INDEX zac_nosuchrel IS 'x'")
                    .contains("relation \"zac_nosuchrel\" does not exist"));
        } finally {
            exec("DROP VIEW zac_vw");
            exec("DROP SEQUENCE zac_sq");
            exec("DROP TABLE zac_t CASCADE");
        }
    }

    /** The kind is a word from a list, the target is one target, and the text is one constant. */
    @Test
    void theCommentGrammarIsTheOnePostgresqlHas() throws SQLException {
        exec("CREATE TABLE zac_g (id int)");
        try {
            assertEquals("42601", stateOf("COMMENT ON BOGUSKIND zac_g IS 'x'"));
            assertEquals("42601", stateOf("COMMENT ON zac_g IS 'x'"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g, zac_g IS 'x'"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g IS 1"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g IS true"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g IS DEFAULT"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g IS 'a' || 'b'"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g IS 'x' 'y'"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g IS 'x' EXTRA"));
            assertEquals("42601", stateOf("COMMENT ON TABLE zac_g"));
            assertTrue(messageOf("COMMENT ON TABLE zac_g").contains("syntax error at end of input"));
            // A column is named by its relation and then by itself.
            assertEquals("42601", stateOf("COMMENT ON COLUMN zac_g IS 'x'"));
            assertTrue(messageOf("COMMENT ON COLUMN zac_g IS 'x'")
                    .contains("column name must be qualified"));
            exec("COMMENT ON COLUMN zac_g.id IS 'the id'");
            assertEquals("the id", one("SELECT col_description('zac_g'::regclass, 1)"));
        } finally {
            exec("DROP TABLE zac_g");
        }
    }

    /** A cast is named by the two types it converts between, and both have to be there. */
    @Test
    void aCastIsNamedByTheTypesItConvertsBetween() throws SQLException {
        exec("COMMENT ON CAST (int4 AS int8) IS 'widening'");
        try {
            assertEquals("widening", one("SELECT obj_description(oid,'pg_cast') FROM pg_cast"
                    + " WHERE castsource='int4'::regtype AND casttarget='int8'::regtype"));
            assertEquals("42704", stateOf("COMMENT ON CAST (int AS zac_nosuchtype) IS 'x'"));
            assertTrue(messageOf("COMMENT ON CAST (int AS zac_nosuchtype) IS 'x'")
                    .contains("type \"zac_nosuchtype\" does not exist"));
            // A conversion nobody registered describes nothing.
            assertTrue(messageOf("COMMENT ON CAST (int4 AS xml) IS 'x'")
                    .contains("cast from type integer to type xml does not exist"));
            assertEquals("42601", stateOf("COMMENT ON CAST (int) IS 'x'"));
            assertEquals("42601", stateOf("COMMENT ON CAST int AS int8 IS 'x'"));
        } finally {
            exec("COMMENT ON CAST (int4 AS int8) IS NULL");
        }
    }

    /** An operator is told apart by its operands as well as by its spelling. */
    @Test
    void anOperatorIsNamedBySpellingAndOperands() throws SQLException {
        exec("CREATE FUNCTION zac_add(int,int) RETURNS int LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT $1+$2 $$");
        exec("CREATE OPERATOR ###@ (LEFTARG=int, RIGHTARG=int, FUNCTION=zac_add)");
        try {
            exec("COMMENT ON OPERATOR ###@ (int,int) IS 'adds them'");
            assertEquals("adds them", one("SELECT obj_description(oid,'pg_operator')"
                    + " FROM pg_operator WHERE oprname='###@'"));
            assertEquals("42883", stateOf("COMMENT ON OPERATOR #%^&* (int, int) IS 'x'"));
            assertTrue(messageOf("COMMENT ON OPERATOR #%^&* (int, int) IS 'x'")
                    .contains("operator does not exist: integer #%^&* integer"));
            // The operands are not optional; there is no operator without them.
            assertEquals("42601", stateOf("COMMENT ON OPERATOR ###@ IS 'x'"));
            // A built-in is reachable the same way.
            assertNull(stateOf("COMMENT ON OPERATOR + (int, int) IS 'sum'"));
        } finally {
            exec("COMMENT ON OPERATOR + (int, int) IS NULL");
            exec("DROP OPERATOR ###@ (int,int)");
            exec("DROP FUNCTION zac_add(int,int)");
        }
    }

    /** Every kind a comment reaches is looked for where that kind of object lives. */
    @Test
    void everyKindIsLookedForWhereThatKindLives() {
        assertEquals("42704", stateOf("COMMENT ON STATISTICS zac_nosuchst IS 'x'"));
        assertEquals("42704", stateOf("COMMENT ON TABLESPACE zac_nosuchts IS 'x'"));
        assertEquals("42704", stateOf("COMMENT ON SERVER zac_nosuch IS 'x'"));
        assertEquals("42704", stateOf("COMMENT ON PUBLICATION zac_nosuch IS 'x'"));
        assertEquals("42704", stateOf("COMMENT ON ACCESS METHOD zac_nosuch IS 'x'"));
        assertEquals("42704", stateOf("COMMENT ON TEXT SEARCH CONFIGURATION zac_nosuch IS 'x'"));
        assertEquals("42704", stateOf("COMMENT ON COLLATION zac_nosuchcoll IS 'x'"));
        assertTrue(messageOf("COMMENT ON OPERATOR CLASS zac_nosuch USING btree IS 'x'")
                .contains("operator class \"zac_nosuch\" does not exist for access method \"btree\""));
        assertTrue(messageOf("COMMENT ON OPERATOR FAMILY zac_nosuch USING btree IS 'x'")
                .contains("operator family \"zac_nosuch\" does not exist for access method \"btree\""));
        assertTrue(messageOf("COMMENT ON TRANSFORM FOR int LANGUAGE sql IS 'x'")
                .contains("transform for type integer language \"sql\" does not exist"));
        assertEquals("42883", stateOf("COMMENT ON AGGREGATE zac_nosuchagg(int) IS 'x'"));
        assertEquals("42601", stateOf("COMMENT ON AGGREGATE zac_nosuchagg IS 'x'"));
    }

    /** A trigger, a rule and a policy are named against a relation, and are there or are not. */
    @Test
    void aRelationScopedObjectIsLookedForOnItsRelation() throws SQLException {
        exec("CREATE TABLE zac_s (id int)");
        try {
            assertEquals("42P01", stateOf("COMMENT ON CONSTRAINT c ON zac_nosuchrel IS 'x'"));
            assertTrue(messageOf("COMMENT ON TRIGGER tg ON zac_s IS 'x'")
                    .contains("trigger \"tg\" for table \"zac_s\" does not exist"));
            assertTrue(messageOf("COMMENT ON RULE r ON zac_s IS 'x'")
                    .contains("rule \"r\" for relation \"zac_s\" does not exist"));
            assertTrue(messageOf("COMMENT ON POLICY p ON zac_s IS 'x'")
                    .contains("policy \"p\" for table \"zac_s\" does not exist"));
        } finally {
            exec("DROP TABLE zac_s");
        }
    }

    /** A comment is part of a definition, so it is written in a transaction and goes with it. */
    @Test
    void aCommentIsWrittenInsideTheTransactionThatWroteIt() throws SQLException {
        exec("CREATE TABLE zac_x (id int)");
        try {
            conn.setAutoCommit(false);
            exec("COMMENT ON TABLE zac_x IS 'rolled back'");
            conn.rollback();
            conn.setAutoCommit(true);
            assertEquals("t", one("SELECT obj_description('zac_x'::regclass) IS NULL"));
            exec("COMMENT ON TABLE zac_x IS 'kept'");
            assertEquals("kept", one("SELECT obj_description('zac_x'::regclass)"));
        } finally {
            conn.setAutoCommit(true);
            exec("DROP TABLE zac_x");
        }
    }

    /** A relation keeps the case it was created with, and its comment describes that relation. */
    @Test
    void aCommentOnAQuotedNameDescribesThatRelation() throws SQLException {
        exec("CREATE TABLE \"zac_Cap\" (id int)");
        try {
            exec("COMMENT ON TABLE \"zac_Cap\" IS 'kept'");
            assertEquals("kept", one("SELECT obj_description('\"zac_Cap\"'::regclass)"));
            assertEquals("1", one("SELECT count(*)::int FROM pg_description"
                    + " WHERE description='kept'"));
        } finally {
            exec("DROP TABLE \"zac_Cap\"");
        }
    }

    /** A comment on a role is a comment on something the whole cluster shares. */
    @Test
    void aRoleComesUnderTheSharedDescriptions() throws SQLException {
        exec("CREATE ROLE zac_r NOLOGIN");
        try {
            exec("COMMENT ON ROLE zac_r IS 'the role'");
            assertEquals("1", one("SELECT count(*)::int FROM pg_shdescription"
                    + " WHERE description='the role'"));
            assertEquals("0", one("SELECT count(*)::int FROM pg_description"
                    + " WHERE description='the role'"));
        } finally {
            exec("DROP ROLE zac_r");
        }
    }

    /** A column's comment is filed against its attribute number, not its place in the list. */
    @Test
    void aColumnCommentIsFiledAgainstItsAttributeNumber() throws SQLException {
        exec("CREATE TABLE zac_c3 (a int, b int, c int)");
        exec("ALTER TABLE zac_c3 DROP COLUMN b");
        try {
            exec("COMMENT ON COLUMN zac_c3.c IS 'ccc'");
            assertEquals("3", one("SELECT objsubid FROM pg_description"
                    + " WHERE objoid='zac_c3'::regclass"));
            assertEquals("ccc/null", one("SELECT col_description('zac_c3'::regclass,3),"
                    + " col_description('zac_c3'::regclass,2)"));
        } finally {
            exec("DROP TABLE zac_c3");
        }
    }

    /** A statistics object takes a comment, and it reaches the catalogue. */
    @Test
    void aStatisticsObjectTakesAComment() throws SQLException {
        exec("CREATE TABLE zac_t2 (a int, b int)");
        exec("CREATE STATISTICS zac_st ON a,b FROM zac_t2");
        try {
            exec("COMMENT ON STATISTICS zac_st IS 'correlated'");
            assertEquals("1", one("SELECT count(*)::int FROM pg_description"
                    + " WHERE description='correlated'"));
        } finally {
            exec("DROP TABLE zac_t2 CASCADE");
        }
    }

    /**
     * A label goes to a provider, and a server with none loaded has nowhere to put one. The
     * statement is still read in full first, so a malformed one is malformed.
     */
    @Test
    void aSecurityLabelIsReadBeforeItIsRefused() throws SQLException {
        exec("CREATE TABLE zac_l (id int)");
        try {
            assertEquals("22023", stateOf("SECURITY LABEL ON TABLE zac_l IS 'x'"));
            assertTrue(messageOf("SECURITY LABEL ON TABLE zac_l IS 'x'")
                    .contains("no security label providers have been loaded"));
            assertTrue(messageOf("SECURITY LABEL FOR anything ON TABLE zac_l IS 'x'")
                    .contains("security label provider \"anything\" is not loaded"));
            // The kinds a label reaches are fewer than the kinds a comment reaches.
            assertEquals("42601", stateOf("SECURITY LABEL ON zac_l IS 'x'"));
            assertEquals("42601", stateOf("SECURITY LABEL ON TABLE IS 'x'"));
            assertEquals("42601", stateOf("SECURITY LABEL ON TABLE zac_l"));
            assertEquals("42601", stateOf("SECURITY LABEL ON TABLE zac_l IS"));
            assertEquals("42601", stateOf("SECURITY LABEL ON TABLE zac_l IS 1"));
            assertEquals("42601", stateOf("SECURITY LABEL FOR ON TABLE zac_l IS 'x'"));
            assertEquals("42601", stateOf("SECURITY LABEL ON TRIGGER tr ON zac_l IS 'x'"));
            assertEquals("42601", stateOf("SECURITY LABEL ON OPERATOR + (int, int) IS 'x'"));
            assertEquals("42601", stateOf("SECURITY LABEL ON BOGUS zac_l IS 'x'"));
            // A name the label never gets as far as looking at is not looked at.
            assertEquals("22023", stateOf("SECURITY LABEL ON TABLE zac_nosuch IS 'x'"));
            assertEquals("22023", stateOf("SECURITY LABEL ON COLUMN zac_l IS 'x'"));
        } finally {
            exec("DROP TABLE zac_l");
        }
    }
}
