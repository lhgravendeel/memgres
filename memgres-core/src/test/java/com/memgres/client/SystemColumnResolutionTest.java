package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A system column is a column of the relation that stores the row.
 *
 * <p>A relation that keeps the rows it hands out carries six columns nobody declared: where the
 * tuple sits, which relation holds it, and which transaction and command wrote and deleted it. A
 * relation that composes its rows on the way past carries none of them, because its rows are not
 * anywhere. They were being treated instead as a fixed feature of whatever the first FROM item
 * happened to be: a sub-SELECT, a CTE, a VALUES list, an ordinary view and a function scan all
 * answered for {@code ctid}; an unqualified one was never ambiguous across two relations that
 * both had it, and was read straight off a join that exposes neither side's; an unmatched
 * outer-join side reported a position for a row it does not have; a grouping did not mask one
 * out; and the DDL that PostgreSQL refuses outright -- an index, a key, a rename, a drop, a
 * generated column -- went through or failed as though the column were simply missing.
 */
class SystemColumnResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE sysc_a (id int, v text)");
            st.execute("CREATE TABLE sysc_b (id int, w text)");
            st.execute("INSERT INTO sysc_a VALUES (1, 'one'), (2, 'two'), (3, 'three')");
            st.execute("INSERT INTO sysc_b VALUES (1, 'uno'), (2, 'dos')");
            st.execute("CREATE VIEW sysc_v AS SELECT id, v FROM sysc_a");
            st.execute("CREATE MATERIALIZED VIEW sysc_mv AS SELECT id, v FROM sysc_a");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
            return out;
        }
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    @Test
    void aStoredRowCarriesTheSystemColumns() throws Exception {
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT ctid::text FROM sysc_a ORDER BY id"));
        assertEquals(List.of("1|sysc_a", "2|sysc_a", "3|sysc_a"),
                rows("SELECT id, tableoid::regclass::text FROM sysc_a ORDER BY id"));
        assertEquals(List.of("1|t|t", "2|t|t", "3|t|t"),
                rows("SELECT id, xmin IS NOT NULL, xmax IS NOT NULL FROM sysc_a ORDER BY id"));
        assertEquals(List.of("1|t|t", "2|t|t", "3|t|t"),
                rows("SELECT id, cmin IS NOT NULL, cmax IS NOT NULL FROM sysc_a ORDER BY id"));
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT sysc_a.ctid::text FROM sysc_a ORDER BY id"));
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT a.ctid::text FROM sysc_a a ORDER BY a.id"));
    }

    /** A materialized view keeps its rows where an ordinary view composes them each time. */
    @Test
    void aMaterializedViewKeepsItsRowsAndAnOrdinaryViewDoesNot() throws Exception {
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT ctid::text FROM sysc_mv ORDER BY id"));
        assertEquals("42703", stateOf("SELECT ctid FROM sysc_v"));
    }

    @Test
    void aRelationThatComposesItsRowsHasNone() {
        assertEquals("42703", stateOf("SELECT ctid FROM (SELECT id FROM sysc_a) s"));
        assertEquals("42703", stateOf("SELECT s.ctid FROM (SELECT id FROM sysc_a) s"));
        assertEquals("42703", stateOf("WITH c AS (SELECT id FROM sysc_a) SELECT ctid FROM c"));
        assertEquals("42703", stateOf("SELECT ctid FROM (VALUES (1), (2)) t(x)"));
        assertEquals("42703", stateOf("SELECT tableoid FROM generate_series(1, 3) g"));
        assertEquals("42703", stateOf("SELECT xmin FROM (SELECT id FROM sysc_a) s"));
    }

    /** With nothing there to have offered it, the name is an ordinary missing column. */
    @Test
    void aDerivedRelationSuggestsItsOwnColumnsInstead() {
        org.postgresql.util.ServerErrorMessage e =
                fieldsOf("SELECT ctid FROM (SELECT id FROM sysc_a) s");
        assertEquals("column \"ctid\" does not exist", e.getMessage());
        assertEquals("Perhaps you meant to reference the column \"s.id\".", e.getHint());
    }

    /** Two FROM items the query listed both offer it, so the name says which of them is meant. */
    @Test
    void unqualifiedItIsSearchedAcrossTheFromItemsTheQueryListed() throws Exception {
        assertEquals("42702", stateOf("SELECT ctid FROM sysc_a, sysc_b"));
        assertEquals("42702", stateOf("SELECT xmin FROM sysc_a, sysc_b"));
        assertEquals("42702", stateOf("SELECT tableoid FROM sysc_a, sysc_b"));
        assertEquals(List.of("(0,1)", "(0,1)", "(0,2)", "(0,2)", "(0,3)", "(0,3)"),
                rows("SELECT ctid::text FROM sysc_a, (SELECT id FROM sysc_b) s ORDER BY 1"));
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT ctid::text FROM sysc_a, (VALUES (1)) t(x) ORDER BY 1"));
    }

    /** A join is one FROM item, and what it exposes is its sides' ordinary columns. */
    @Test
    void aJoinExposesNeitherSidesSystemColumns() throws Exception {
        assertEquals("42703", stateOf("SELECT ctid FROM sysc_a a JOIN sysc_b b ON a.id = b.id"));
        assertEquals("42703",
                stateOf("SELECT ctid FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id"));
        assertEquals("42703", stateOf("SELECT ctid FROM sysc_a CROSS JOIN sysc_b"));
        assertEquals("42703",
                stateOf("SELECT tableoid FROM sysc_a a JOIN sysc_b b ON a.id = b.id"));
        assertEquals("42703",
                stateOf("SELECT ctid FROM sysc_a a JOIN (SELECT id FROM sysc_b) b ON a.id = b.id"));
        assertEquals(List.of("(0,1)", "(0,2)"),
                rows("SELECT a.ctid::text FROM sysc_a a JOIN sysc_b b ON a.id = b.id"
                        + " ORDER BY a.id"));
        assertEquals(List.of("(0,1)|(0,1)", "(0,2)|(0,2)"),
                rows("SELECT a.ctid::text, b.ctid::text FROM sysc_a a"
                        + " JOIN sysc_b b ON a.id = b.id ORDER BY a.id"));
    }

    /** The relations are still there to be named one at a time, which is what the error says. */
    @Test
    void aJoinPointsAtTheRelationsThatStillHaveIt() {
        org.postgresql.util.ServerErrorMessage both =
                fieldsOf("SELECT ctid FROM sysc_a a JOIN sysc_b b ON a.id = b.id");
        assertEquals("column \"ctid\" does not exist", both.getMessage());
        assertEquals("There are columns named \"ctid\", but they are in tables that cannot be"
                + " referenced from this part of the query.", both.getDetail());
        assertEquals("Try using a table-qualified name.", both.getHint());

        org.postgresql.util.ServerErrorMessage one = fieldsOf(
                "SELECT ctid FROM sysc_a a JOIN (SELECT id FROM sysc_b) b ON a.id = b.id");
        assertEquals("There is a column named \"ctid\" in table \"a\", but it cannot be"
                + " referenced from this part of the query.", one.getDetail());
        assertEquals("To reference that column, you must use a table-qualified name.",
                one.getHint());
    }

    /** An unmatched side is a relation with no row, so its system columns are null with it. */
    @Test
    void anUnmatchedOuterJoinSideHasNoneOfThem() throws Exception {
        assertEquals(List.of("1|f", "2|f", "3|t"),
                rows("SELECT a.id, b.ctid IS NULL FROM sysc_a a"
                        + " LEFT JOIN sysc_b b ON a.id = b.id ORDER BY a.id"));
        assertEquals(List.of("1|f", "2|f", "3|t"),
                rows("SELECT a.id, b.tableoid IS NULL FROM sysc_a a"
                        + " LEFT JOIN sysc_b b ON a.id = b.id ORDER BY a.id"));
        assertEquals(List.of("1|f|f", "2|f|f", "3|t|t"),
                rows("SELECT a.id, b.xmin IS NULL, b.cmin IS NULL FROM sysc_a a"
                        + " LEFT JOIN sysc_b b ON a.id = b.id ORDER BY a.id"));
        assertEquals(List.of("3"),
                rows("SELECT a.id FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id"
                        + " WHERE b.ctid IS NULL ORDER BY a.id"));
        assertEquals(List.of("2"),
                rows("SELECT count(*) FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id"
                        + " WHERE b.ctid IS NOT NULL"));
    }

    /** A ctid varies within a group, so naming it outside the GROUP BY asks for what it has not. */
    @Test
    void aGroupingMasksOneOutAsItMasksOutADeclaredColumn() throws Exception {
        assertEquals("42803", stateOf("SELECT ctid FROM sysc_a GROUP BY id"));
        assertEquals("42803", stateOf("SELECT id FROM sysc_a GROUP BY id ORDER BY ctid"));
        assertEquals("42803",
                stateOf("SELECT count(*) FROM sysc_a GROUP BY id HAVING ctid IS NOT NULL"));
        assertEquals("column \"sysc_a.ctid\" must appear in the GROUP BY clause or be used in"
                        + " an aggregate function",
                fieldsOf("SELECT ctid FROM sysc_a GROUP BY id").getMessage());
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT ctid::text FROM sysc_a GROUP BY ctid ORDER BY 1"));
        assertEquals(List.of("1", "1", "1"),
                rows("SELECT count(*) FROM sysc_a GROUP BY ctid ORDER BY 1"));
        assertEquals(List.of("1", "2", "3"),
                rows("SELECT max(id) FROM sysc_a GROUP BY ctid ORDER BY 1"));
    }

    @Test
    void oneMayBeWrittenWhereverAColumnMayBe() throws Exception {
        assertEquals(List.of("(0,2)"), rows("SELECT ctid::text FROM sysc_a WHERE id = 2"));
        assertEquals(List.of("1", "2", "3"),
                rows("SELECT id FROM sysc_a WHERE ctid IS NOT NULL ORDER BY id"));
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT ctid::text FROM sysc_a ORDER BY ctid"));
        assertEquals(List.of("(0,1)", "(0,1)", "(0,1)"),
                rows("SELECT (SELECT ctid::text FROM sysc_b WHERE id = 1) FROM sysc_a"
                        + " ORDER BY id"));
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)", "(0,1)", "(0,2)"),
                rows("SELECT ctid::text FROM sysc_a UNION ALL SELECT ctid::text FROM sysc_b"));
    }

    /** Reading one is a column reference; writing one is not, because nobody declared it. */
    @Test
    void noneOfThemIsWrittenTo() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE sysc_w (id int, v text)");
            st.execute("INSERT INTO sysc_w VALUES (1, 'one'), (2, 'two'), (3, 'three')");
        }
        assertEquals("42703", stateOf("INSERT INTO sysc_w (ctid) VALUES ('(0,1)')"));
        assertEquals("0A000", stateOf("UPDATE sysc_w SET ctid = '(0,1)'"));
        assertEquals(List.of("1", "2", "3"),
                rows("UPDATE sysc_w SET v = v WHERE ctid IS NOT NULL RETURNING id"));
    }

    /**
     * An index reads what a row is, and a system column says where it is instead. The two with no
     * btree operator class behind them are refused for that, before the rule about system columns
     * is ever reached.
     */
    @Test
    void noIndexIsBuiltOverOne() {
        assertEquals("0A000", stateOf("CREATE INDEX sysc_i ON sysc_a (ctid)"));
        assertEquals("0A000", stateOf("CREATE UNIQUE INDEX sysc_i ON sysc_a (tableoid)"));
        assertEquals("42704", stateOf("CREATE INDEX sysc_i ON sysc_a (xmin)"));
        assertEquals("42704", stateOf("CREATE INDEX sysc_i ON sysc_a (cmin)"));
        assertEquals("0A000", stateOf("CREATE INDEX sysc_i ON sysc_a (id) INCLUDE (ctid)"));
        assertEquals("0A000", stateOf("CREATE INDEX sysc_i ON sysc_a ((ctid::text))"));
        assertEquals("0A000",
                stateOf("CREATE INDEX sysc_i ON sysc_a (id) WHERE ctid IS NOT NULL"));
        assertEquals("data type xid has no default operator class for access method \"btree\"",
                fieldsOf("CREATE INDEX sysc_i ON sysc_a (xmin)").getMessage());
        assertEquals("index creation on system columns is not supported",
                fieldsOf("CREATE INDEX sysc_i ON sysc_a (ctid)").getMessage());
    }

    @Test
    void noKeyIsDeclaredOverOne() {
        assertEquals("0A000", stateOf("CREATE TABLE sysc_k (id int, UNIQUE (ctid))"));
        assertEquals("0A000", stateOf("CREATE TABLE sysc_k (id int, PRIMARY KEY (tableoid))"));
        assertEquals("42704", stateOf("CREATE TABLE sysc_k (id int, UNIQUE (xmax))"));
        assertEquals("0A000", stateOf("ALTER TABLE sysc_a ADD PRIMARY KEY (ctid)"));
        assertEquals("0A000", stateOf("ALTER TABLE sysc_a ADD UNIQUE (ctid)"));
        assertEquals("42704", stateOf("ALTER TABLE sysc_a ADD UNIQUE (xmin)"));
        // A primary key makes its columns NOT NULL before it builds anything.
        assertEquals("cannot add not-null constraint on system column \"ctid\"",
                fieldsOf("ALTER TABLE sysc_a ADD PRIMARY KEY (ctid)").getMessage());
    }

    /** An arbiter resolves the name and finds no unique index was ever built over it. */
    @Test
    void noArbiterIsFoundForOne() {
        assertEquals("42P10",
                stateOf("INSERT INTO sysc_a VALUES (9, 'nine') ON CONFLICT (ctid) DO NOTHING"));
    }

    /** They are there because the row is stored, so no ALTER TABLE has any say over them. */
    @Test
    void noAlterTableDropsRenamesOrAltersOne() {
        assertEquals("0A000", stateOf("ALTER TABLE sysc_a DROP COLUMN ctid"));
        assertEquals("0A000", stateOf("ALTER TABLE sysc_a DROP COLUMN IF EXISTS xmin"));
        assertEquals("0A000", stateOf("ALTER TABLE sysc_a RENAME COLUMN ctid TO z"));
        assertEquals("0A000", stateOf("ALTER TABLE sysc_a ALTER COLUMN ctid SET NOT NULL"));
        assertEquals("0A000", stateOf("ALTER TABLE sysc_a ALTER COLUMN xmin TYPE int"));
        // IF EXISTS does not soften it: the column is very much there, which is why it cannot go.
        assertEquals("cannot drop system column \"xmin\"",
                fieldsOf("ALTER TABLE sysc_a DROP COLUMN IF EXISTS xmin").getMessage());
    }

    /** A generated column is settled while the row is written, when it has none of these yet. */
    @Test
    void noGeneratedColumnIsComputedFromOne() {
        assertEquals("42P10", stateOf(
                "CREATE TABLE sysc_g (id int, c int GENERATED ALWAYS AS (xmin::text::int) STORED)"));
        assertEquals("42P10", stateOf(
                "CREATE TABLE sysc_g (id int, c text GENERATED ALWAYS AS (ctid::text) STORED)"));
        assertEquals("42P10", stateOf(
                "ALTER TABLE sysc_a ADD COLUMN c text GENERATED ALWAYS AS (ctid::text) STORED"));
        assertEquals("42P10", stateOf("CREATE TABLE sysc_g (id int, CHECK (ctid IS NOT NULL))"));
        assertEquals("cannot use system column \"ctid\" in column generation expression",
                fieldsOf("CREATE TABLE sysc_g (id int,"
                        + " c text GENERATED ALWAYS AS (ctid::text) STORED)").getMessage());
    }
}
