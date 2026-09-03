package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The two forms a definition is written back in, and the one reader that unwraps a stored tree.
 *
 * <p>The plain form parenthesises every operator, so that the text reads back as the tree it came
 * from whatever the reader knows of precedence. The pretty form is for a person and leaves out the
 * parentheses precedence makes unnecessary: {@code CHECK (id > 0 AND id < 100)} where the plain
 * form writes {@code CHECK (((id > 0) AND (id < 100)))}.
 *
 * <p>What pg_constraint keeps in conbin is the tree itself, which nothing but {@code pg_get_expr}
 * reads; asked for it, that function answers with the expression and not with the storage.
 */
class HowADefinitionIsWrittenBackTest {

    private static Memgres memgres;
    private static Connection conn;

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

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static String prettyDef(String constraintName) throws SQLException {
        return one("SELECT pg_get_constraintdef(oid, true) FROM pg_constraint"
                + " WHERE conname = '" + constraintName + "'");
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** A check written back in both forms. */
    @Test
    void theTwoFormsACheckIsWrittenIn() throws SQLException {
        exec("CREATE TABLE zhd_g (id int, CONSTRAINT zhd_ck CHECK (id > 0 AND id < 100))");
        assertEquals("CHECK (((id > 0) AND (id < 100)))",
                one("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='zhd_ck'"));
        assertEquals("CHECK (id > 0 AND id < 100)",
                one("SELECT pg_get_constraintdef(oid, true) FROM pg_constraint"
                        + " WHERE conname='zhd_ck'"));
        // The false form is the plain one, written out in full.
        assertEquals("CHECK (((id > 0) AND (id < 100)))",
                one("SELECT pg_get_constraintdef(oid, false) FROM pg_constraint"
                        + " WHERE conname='zhd_ck'"));
        // The tree kept in conbin is read back as the expression it holds.
        assertEquals("((id > 0) AND (id < 100))",
                one("SELECT pg_get_expr(conbin, conrelid) FROM pg_constraint"
                        + " WHERE conname='zhd_ck'"));
        exec("DROP TABLE zhd_g");
    }

    /** A trigger's condition, written back in both forms. */
    @Test
    void theTwoFormsAConditionIsWrittenIn() throws SQLException {
        exec("CREATE TABLE zhd_t (id int)");
        exec("CREATE FUNCTION zhd_f() RETURNS trigger LANGUAGE plpgsql"
                + " AS $$ BEGIN RETURN NEW; END $$");
        exec("CREATE TRIGGER zhd_tg BEFORE INSERT ON zhd_t FOR EACH ROW"
                + " WHEN (new.id > 0) EXECUTE FUNCTION zhd_f()");
        assertTrue(one("SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname='zhd_tg'")
                .contains("WHEN ((new.id > 0))"));
        assertTrue(one("SELECT pg_get_triggerdef(oid, true) FROM pg_trigger"
                + " WHERE tgname='zhd_tg'").contains("WHEN (new.id > 0)"));
        exec("DROP TRIGGER zhd_tg ON zhd_t");
        exec("DROP FUNCTION zhd_f()");
        exec("DROP TABLE zhd_t");
    }

    /** A policy's condition is written back by the same deparser. */
    @Test
    void howAPolicysConditionIsWrittenBack() throws SQLException {
        exec("CREATE TABLE zhd_p (owner text, n int)");
        exec("ALTER TABLE zhd_p ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY zhd_pp ON zhd_p USING (owner = CURRENT_USER AND n > 5)");
        // CURRENT_USER is a word SQL spells without parentheses, and is written back as one.
        assertEquals("((owner = CURRENT_USER) AND (n > 5))",
                one("SELECT qual FROM pg_policies WHERE policyname='zhd_pp'"));
        exec("CREATE POLICY zhd_pw ON zhd_p FOR INSERT WITH CHECK (n > 0)");
        assertEquals("(n > 0)",
                one("SELECT with_check FROM pg_policies WHERE policyname='zhd_pw'"));
        exec("DROP TABLE zhd_p");
    }

    /** A check over arithmetic keeps the parentheses its meaning needs. */
    @Test
    void theParenthesesTheMeaningNeeds() throws SQLException {
        exec("CREATE TABLE zhd_a (a int, b int, c int)");
        exec("ALTER TABLE zhd_a ADD CONSTRAINT zhd_ck2 CHECK ((a + b) * 2 > 0)");
        exec("ALTER TABLE zhd_a ADD CONSTRAINT zhd_ck3 CHECK (a + b * 2 > 0)");
        exec("ALTER TABLE zhd_a ADD CONSTRAINT zhd_ck4 CHECK (a - b - c > 0)");
        exec("ALTER TABLE zhd_a ADD CONSTRAINT zhd_ck5 CHECK (a - (b - c) > 0)");
        exec("ALTER TABLE zhd_a ADD CONSTRAINT zhd_ck6 CHECK (a BETWEEN 1 AND 5)");
        // An operator that is not one of the five PostgreSQL reads for this keeps its own pair.
        assertEquals("CHECK (((a + b) * 2) > 0)", prettyDef("zhd_ck2"));
        // Within them, what binds tighter needs none: a multiplication under an addition, and
        // the left of two that bind equally.
        assertEquals("CHECK ((a + b * 2) > 0)", prettyDef("zhd_ck3"));
        assertEquals("CHECK ((a - b - c) > 0)", prettyDef("zhd_ck4"));
        assertEquals("CHECK ((a - (b - c)) > 0)", prettyDef("zhd_ck5"));
        // BETWEEN is a pair of comparisons under an AND, and loses its parentheses with it.
        assertEquals("CHECK (a >= 1 AND a <= 5)", prettyDef("zhd_ck6"));
        exec("DROP TABLE zhd_a");
    }
}
