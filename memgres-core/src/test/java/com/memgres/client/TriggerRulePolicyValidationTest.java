package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A trigger's WHEN condition, a rule's action and a policy's USING expression are written once
 * and evaluated much later, so a definition that cannot work leaves no trace until it is used —
 * at which point the failure surfaces in the middle of someone else's DML. PostgreSQL resolves
 * all three against the target relation while they are being defined; these tests pin the
 * rejections, and the neighbouring definitions that must keep being accepted.
 */
class TriggerRulePolicyValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE trp_t (i int, j text)");
        exec("CREATE TABLE trp_log (m text)");
        exec("CREATE VIEW trp_v AS SELECT i, j FROM trp_t");
        exec("CREATE FUNCTION trp_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION trp_notrig() RETURNS int AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql");
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

    /** Assert the statement is refused with this SQLSTATE and a message containing this text. */
    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- CREATE TRIGGER ----

    @Test
    void aTriggerFunctionMustExistAndReturnTrigger() {
        assertRejected("42883", "trp_nosuch",
                "CREATE TRIGGER trp_g1 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_nosuch()");
        assertRejected("42P17", "must return type trigger",
                "CREATE TRIGGER trp_g2 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_notrig()");
    }

    @Test
    void aStatementTriggersWhenConditionHasNoRowToLookAt() {
        assertRejected("42P17", "statement trigger's WHEN condition cannot reference column values",
                "CREATE TRIGGER trp_g3 AFTER INSERT ON trp_t FOR EACH STATEMENT WHEN (NEW.i > 0) "
                        + "EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void theEventDecidesWhichOfOldAndNewAWhenConditionMayName() {
        assertRejected("42P17", "INSERT trigger's WHEN condition cannot reference OLD values",
                "CREATE TRIGGER trp_g4 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (OLD.i > 0) "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("42P17", "DELETE trigger's WHEN condition cannot reference NEW values",
                "CREATE TRIGGER trp_g5 BEFORE DELETE ON trp_t FOR EACH ROW WHEN (NEW.i > 0) "
                        + "EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void insteadOfBelongsToAViewAndRowTriggersToATable() {
        assertRejected("42809", "\"trp_t\" is a table",
                "CREATE TRIGGER trp_g6 INSTEAD OF INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        assertRejected("42809", "\"trp_v\" is a view",
                "CREATE TRIGGER trp_g7 BEFORE INSERT ON trp_v FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        assertRejected("0A000", "INSTEAD OF triggers must be FOR EACH ROW",
                "CREATE TRIGGER trp_g8 INSTEAD OF INSERT ON trp_v FOR EACH STATEMENT "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("0A000", "INSTEAD OF triggers cannot have WHEN conditions",
                "CREATE TRIGGER trp_g9 INSTEAD OF INSERT ON trp_v FOR EACH ROW WHEN (true) "
                        + "EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void updateOfNamesColumnsOfTheRelation() {
        assertRejected("42703", "column \"nosuch\" of relation \"trp_t\" does not exist",
                "CREATE TRIGGER trp_g10 BEFORE UPDATE OF nosuch ON trp_t FOR EACH ROW "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("42703", "column \"nosuch\" of relation \"trp_t\" does not exist",
                "CREATE TRIGGER trp_g11 BEFORE UPDATE OF i, nosuch ON trp_t FOR EACH ROW "
                        + "EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void truncateHasNoPerRowFiringToDo() {
        assertRejected("0A000", "TRUNCATE FOR EACH ROW triggers are not supported",
                "CREATE TRIGGER trp_g12 BEFORE TRUNCATE ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void aWhenConditionIsResolvedAgainstTheTriggersOwnRows() {
        assertRejected("42703", "column new.nosuch does not exist",
                "CREATE TRIGGER trp_g13 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (NEW.nosuch > 0) "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("42804", "argument of WHEN must be type boolean, not type integer",
                "CREATE TRIGGER trp_g14 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (NEW.i) "
                        + "EXECUTE FUNCTION trp_tf()");
        // Both OLD and NEW are in scope, so a bare column name names neither.
        assertRejected("42702", "column reference \"i\" is ambiguous",
                "CREATE TRIGGER trp_g15 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (i > 0) "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("42P01", "missing FROM-clause entry for table \"zz\"",
                "CREATE TRIGGER trp_g16 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (zz.i > 0) "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("42803", "aggregate functions are not allowed in trigger WHEN conditions",
                "CREATE TRIGGER trp_g17 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (count(*) > 0) "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("0A000", "cannot use subquery in trigger WHEN condition",
                "CREATE TRIGGER trp_g18 BEFORE INSERT ON trp_t FOR EACH ROW "
                        + "WHEN ((SELECT count(*) FROM trp_t) > 0) EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void aTransitionTableNeedsAStatementThatHasAlreadyRun() {
        assertRejected("42P17", "transition table name can only be specified for an AFTER trigger",
                "CREATE TRIGGER trp_g19 BEFORE INSERT ON trp_t REFERENCING NEW TABLE AS nt "
                        + "FOR EACH STATEMENT EXECUTE FUNCTION trp_tf()");
        assertRejected("42P17", "OLD TABLE can only be specified for a DELETE or UPDATE trigger",
                "CREATE TRIGGER trp_g20 AFTER INSERT ON trp_t REFERENCING OLD TABLE AS ot "
                        + "FOR EACH STATEMENT EXECUTE FUNCTION trp_tf()");
        assertRejected("42P17", "NEW TABLE can only be specified for an INSERT or UPDATE trigger",
                "CREATE TRIGGER trp_g21 AFTER DELETE ON trp_t REFERENCING NEW TABLE AS nt "
                        + "FOR EACH STATEMENT EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void aConstraintTriggerIsOnlyEverAfterForEachRow() {
        assertRejected("42601", "syntax error at or near \"BEFORE\"",
                "CREATE CONSTRAINT TRIGGER trp_g22 BEFORE INSERT ON trp_t FOR EACH ROW "
                        + "EXECUTE FUNCTION trp_tf()");
        assertRejected("42601", "syntax error at or near \"STATEMENT\"",
                "CREATE CONSTRAINT TRIGGER trp_g23 AFTER INSERT ON trp_t FOR EACH STATEMENT "
                        + "EXECUTE FUNCTION trp_tf()");
    }

    @Test
    void aTriggerNameIsUniquePerRelation() throws Exception {
        exec("CREATE TRIGGER trp_dup BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        assertRejected("42710", "trigger \"trp_dup\" for relation \"trp_t\" already exists",
                "CREATE TRIGGER trp_dup BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        // The same name on another relation is a different trigger.
        exec("CREATE TRIGGER trp_dup BEFORE INSERT ON trp_log FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        exec("DROP TRIGGER trp_dup ON trp_log");
    }

    @Test
    void validTriggerDefinitionsAreStillAccepted() throws Exception {
        exec("CREATE TRIGGER trp_ok1 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        exec("CREATE TRIGGER trp_ok2 AFTER INSERT ON trp_t FOR EACH STATEMENT EXECUTE FUNCTION trp_tf()");
        exec("CREATE TRIGGER trp_ok3 BEFORE UPDATE ON trp_t FOR EACH ROW "
                + "WHEN (OLD.i IS DISTINCT FROM NEW.i) EXECUTE FUNCTION trp_tf()");
        exec("CREATE TRIGGER trp_ok4 BEFORE TRUNCATE ON trp_t FOR EACH STATEMENT EXECUTE FUNCTION trp_tf()");
        exec("CREATE TRIGGER trp_ok5 AFTER INSERT ON trp_t REFERENCING NEW TABLE AS nt "
                + "FOR EACH STATEMENT EXECUTE FUNCTION trp_tf()");
        exec("CREATE TRIGGER trp_ok6 INSTEAD OF INSERT ON trp_v FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        exec("CREATE CONSTRAINT TRIGGER trp_ok7 AFTER INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf()");
        for (String name : new String[]{"trp_ok1", "trp_ok2", "trp_ok3", "trp_ok4", "trp_ok5", "trp_ok7"}) {
            exec("DROP TRIGGER " + name + " ON trp_t");
        }
        exec("DROP TRIGGER trp_ok6 ON trp_v");
    }

    @Test
    void disablingATriggerThatIsNotThereIsAnError() throws Exception {
        assertRejected("42704", "trigger \"trp_no_such\" for table \"trp_t\" does not exist",
                "ALTER TABLE trp_t DISABLE TRIGGER trp_no_such");
        assertRejected("42704", "trigger \"trp_no_such\" for table \"trp_t\" does not exist",
                "ALTER TABLE trp_t ENABLE TRIGGER trp_no_such");
        // ALL and USER are group selectors, not names, and match nothing without complaint.
        exec("ALTER TABLE trp_t DISABLE TRIGGER ALL");
        exec("ALTER TABLE trp_t ENABLE TRIGGER ALL");
        exec("ALTER TABLE trp_t DISABLE TRIGGER USER");
        exec("ALTER TABLE trp_t ENABLE TRIGGER USER");
    }

    // ---- CREATE RULE ----

    @Test
    void aTableMayNotCarryAnOnSelectRule() {
        assertRejected("42809", "relation \"trp_t\" cannot have ON SELECT rules",
                "CREATE RULE trp_r1 AS ON SELECT TO trp_t DO INSTEAD NOTHING");
        assertRejected("42809", "relation \"trp_t\" cannot have ON SELECT rules",
                "CREATE RULE trp_r2 AS ON SELECT TO trp_t DO INSTEAD SELECT * FROM trp_log");
        assertRejected("42809", "relation \"trp_t\" cannot have ON SELECT rules",
                "CREATE RULE \"_RETURN\" AS ON SELECT TO trp_t DO INSTEAD SELECT * FROM trp_log");
    }

    @Test
    void theEventDecidesWhichOfOldAndNewARuleActionMayName() throws Exception {
        assertRejected("42P17", "ON INSERT rule cannot use OLD",
                "CREATE RULE trp_r3 AS ON INSERT TO trp_t DO ALSO INSERT INTO trp_log VALUES (OLD.j)");
        assertRejected("42P17", "ON DELETE rule cannot use NEW",
                "CREATE RULE trp_r4 AS ON DELETE TO trp_t DO ALSO INSERT INTO trp_log VALUES (NEW.j)");
        exec("CREATE RULE trp_r5 AS ON UPDATE TO trp_t DO ALSO INSERT INTO trp_log VALUES (OLD.j)");
        exec("CREATE RULE trp_r6 AS ON UPDATE TO trp_t DO ALSO INSERT INTO trp_log VALUES (NEW.j)");
        exec("DROP RULE trp_r5 ON trp_t");
        exec("DROP RULE trp_r6 ON trp_t");
    }

    @Test
    void aRuleNeedsAKnownEventAnExistingRelationAndAFreeName() throws Exception {
        assertRejected("42601", "syntax error at or near \"nonsense\"",
                "CREATE RULE trp_r7 AS ON nonsense TO trp_t DO INSTEAD NOTHING");
        assertRejected("42P01", "relation \"trp_nosuchtbl\" does not exist",
                "CREATE RULE trp_r8 AS ON INSERT TO trp_nosuchtbl DO INSTEAD NOTHING");
        exec("CREATE RULE trp_r9 AS ON DELETE TO trp_t DO INSTEAD NOTHING");
        assertRejected("42710", "rule \"trp_r9\" for relation \"trp_t\" already exists",
                "CREATE RULE trp_r9 AS ON DELETE TO trp_t DO INSTEAD NOTHING");
        exec("DROP RULE trp_r9 ON trp_t");
    }

    @Test
    void anActionListThatIsNeverClosedRunsOffTheEnd() {
        assertRejected("42601", "syntax error at end of input",
                "CREATE RULE trp_r10 AS ON UPDATE TO trp_t DO ALSO ( INSERT INTO trp_log VALUES ('u1')");
    }

    @Test
    void aRuleMayHaveSeveralActions() throws Exception {
        exec("CREATE TABLE trp_multi (i int)");
        exec("CREATE RULE trp_r11 AS ON INSERT TO trp_multi DO ALSO "
                + "( INSERT INTO trp_log VALUES ('a'); INSERT INTO trp_log VALUES ('b'); )");
        exec("INSERT INTO trp_multi VALUES (1)");
        assertEquals("2", scalar("SELECT count(*) FROM trp_log WHERE m IN ('a','b')"));
        exec("DELETE FROM trp_log");
        exec("DROP TABLE trp_multi CASCADE");
    }

    @Test
    void aDisabledRuleStopsFiringAndComesBackWhenEnabled() throws Exception {
        exec("CREATE TABLE trp_dis (i int)");
        exec("CREATE RULE trp_r12 AS ON INSERT TO trp_dis DO ALSO INSERT INTO trp_log VALUES ('d')");
        exec("ALTER TABLE trp_dis DISABLE RULE trp_r12");
        exec("INSERT INTO trp_dis VALUES (1)");
        assertEquals("0", scalar("SELECT count(*) FROM trp_log WHERE m = 'd'"));
        exec("ALTER TABLE trp_dis ENABLE RULE trp_r12");
        exec("INSERT INTO trp_dis VALUES (2)");
        assertEquals("1", scalar("SELECT count(*) FROM trp_log WHERE m = 'd'"));
        assertRejected("42704", "rule \"trp_no_such_rule\" for relation \"trp_dis\" does not exist",
                "ALTER TABLE trp_dis DISABLE RULE trp_no_such_rule");
        exec("DELETE FROM trp_log");
        exec("DROP TABLE trp_dis CASCADE");
    }

    @Test
    void setWithoutClusterIsAccepted() throws Exception {
        exec("ALTER TABLE trp_t SET WITHOUT CLUSTER");
    }

    // ---- CREATE POLICY ----

    @Test
    void aPolicyNeedsATableNotAView() {
        assertRejected("42P01", "relation \"trp_nosuchtbl\" does not exist",
                "CREATE POLICY trp_p1 ON trp_nosuchtbl FOR SELECT USING (true)");
        assertRejected("42809", "\"trp_v\" is not a table",
                "CREATE POLICY trp_p2 ON trp_v FOR SELECT USING (true)");
    }

    @Test
    void theCommandDecidesWhichClauseAPolicyMayCarry() {
        assertRejected("42601", "WITH CHECK cannot be applied to SELECT or DELETE",
                "CREATE POLICY trp_p3 ON trp_t FOR SELECT WITH CHECK (true)");
        assertRejected("42601", "WITH CHECK cannot be applied to SELECT or DELETE",
                "CREATE POLICY trp_p4 ON trp_t FOR DELETE WITH CHECK (true)");
        assertRejected("42601", "only WITH CHECK expression allowed for INSERT",
                "CREATE POLICY trp_p5 ON trp_t FOR INSERT USING (true)");
    }

    @Test
    void aPolicyExpressionIsResolvedAgainstTheTableAndMustBeBoolean() {
        assertRejected("42804", "argument of POLICY must be type boolean, not type integer",
                "CREATE POLICY trp_p6 ON trp_t FOR SELECT USING (i)");
        assertRejected("42804", "argument of POLICY must be type boolean, not type text",
                "CREATE POLICY trp_p7 ON trp_t FOR SELECT USING (j)");
        assertRejected("42804", "argument of POLICY must be type boolean, not type integer",
                "CREATE POLICY trp_p8 ON trp_t FOR INSERT WITH CHECK (i)");
        assertRejected("42703", "column \"nosuchcol\" does not exist",
                "CREATE POLICY trp_p9 ON trp_t FOR SELECT USING (nosuchcol = 1)");
        assertRejected("42703", "column trp_t.nosuch does not exist",
                "CREATE POLICY trp_p10 ON trp_t FOR SELECT USING (trp_t.nosuch = 1)");
        assertRejected("42703", "column \"nosuch\" does not exist",
                "CREATE POLICY trp_p11 ON trp_t FOR UPDATE USING (true) WITH CHECK (nosuch > 0)");
        assertRejected("42803", "aggregate functions are not allowed in policy expressions",
                "CREATE POLICY trp_p12 ON trp_t FOR SELECT USING (count(*) > 0)");
    }

    @Test
    void aPolicyNamesRolesThatExistAndOptionsThatAreRecognized() {
        assertRejected("42704", "role \"trp_no_such_role\" does not exist",
                "CREATE POLICY trp_p13 ON trp_t TO trp_no_such_role USING (true)");
        assertRejected("42601", "unrecognized row security option \"nonsense\"",
                "CREATE POLICY trp_p14 ON trp_t AS nonsense FOR SELECT USING (true)");
    }

    @Test
    void aPolicyNameIsUniquePerTable() throws Exception {
        exec("CREATE POLICY trp_pdup ON trp_t FOR SELECT USING (i > 0)");
        assertRejected("42710", "policy \"trp_pdup\" for table \"trp_t\" already exists",
                "CREATE POLICY trp_pdup ON trp_t FOR SELECT USING (i > 0)");
        exec("CREATE POLICY trp_pdup ON trp_log FOR SELECT USING (true)");
        exec("DROP POLICY trp_pdup ON trp_log");
        exec("DROP POLICY trp_pdup ON trp_t");
    }

    @Test
    void validPolicyDefinitionsAreStillAccepted() throws Exception {
        exec("CREATE POLICY trp_pok1 ON trp_t FOR INSERT WITH CHECK (i > 0)");
        exec("CREATE POLICY trp_pok2 ON trp_t USING (i > 0) WITH CHECK (i > 0)");
        exec("CREATE POLICY trp_pok3 ON trp_t AS RESTRICTIVE FOR ALL TO PUBLIC USING (true)");
        exec("CREATE POLICY trp_pok4 ON trp_t FOR UPDATE USING (true) WITH CHECK (true)");
        // A subquery brings its own relations into scope, so an aggregate inside one is fine.
        exec("CREATE POLICY trp_pok5 ON trp_t FOR SELECT USING ((SELECT count(*) FROM trp_log) > 0)");
        exec("CREATE POLICY trp_pok6 ON trp_t TO CURRENT_USER USING (true)");
        for (int n = 1; n <= 6; n++) exec("DROP POLICY trp_pok" + n + " ON trp_t");
    }

    @Test
    void thePolicyCommandCannotBeAltered() throws Exception {
        exec("CREATE POLICY trp_palt ON trp_t FOR SELECT USING (true)");
        assertRejected("42601", "syntax error at or near \"FOR\"",
                "ALTER POLICY trp_palt ON trp_t FOR UPDATE USING (true)");
        exec("ALTER POLICY trp_palt ON trp_t USING (i > 1)");
        exec("DROP POLICY trp_palt ON trp_t");
    }

    // ---- CREATE VIEW ----

    @Test
    void aViewMayNotRepeatAnOutputColumnName() {
        assertRejected("42701", "column \"i\" specified more than once",
                "CREATE VIEW trp_v2 AS SELECT i, i FROM trp_t");
        assertRejected("42701", "column \"a\" specified more than once",
                "CREATE VIEW trp_v2 AS SELECT i AS a, j AS a FROM trp_t");
        assertRejected("42701", "column \"a\" specified more than once",
                "CREATE VIEW trp_v2 (a, a) AS SELECT i, j FROM trp_t");
        assertRejected("42701", "column \"i\" specified more than once",
                "CREATE VIEW trp_v2 AS SELECT a.i, b.i FROM trp_t a, trp_t b");
    }

    @Test
    void aCheckOptionNeedsAViewAnInsertCanReach() {
        assertRejected("0A000", "WITH CHECK OPTION is supported only on automatically updatable views",
                "CREATE VIEW trp_v2 AS SELECT DISTINCT i FROM trp_t WITH CHECK OPTION");
        assertRejected("0A000", "WITH CHECK OPTION is supported only on automatically updatable views",
                "CREATE VIEW trp_v2 AS SELECT count(*) AS c FROM trp_t WITH CHECK OPTION");
        assertRejected("0A000", "WITH CHECK OPTION is supported only on automatically updatable views",
                "CREATE VIEW trp_v2 AS SELECT i FROM trp_t GROUP BY i WITH CHECK OPTION");
        assertRejected("0A000", "WITH CHECK OPTION is supported only on automatically updatable views",
                "CREATE VIEW trp_v2 AS SELECT i FROM trp_t LIMIT 5 WITH CHECK OPTION");
        assertRejected("0A000", "WITH CHECK OPTION is supported only on automatically updatable views",
                "CREATE VIEW trp_v2 AS SELECT a.i FROM trp_t a, trp_t b WITH CHECK OPTION");
    }

    @Test
    void aCheckOptionOnAnUpdatableViewStillWorks() throws Exception {
        exec("CREATE TABLE trp_cv (i int)");
        exec("CREATE VIEW trp_cvv AS SELECT i FROM trp_cv WHERE i > 0 WITH CHECK OPTION");
        exec("INSERT INTO trp_cvv VALUES (5)");
        assertEquals("5", scalar("SELECT i FROM trp_cv"));
        SQLException e = assertThrows(SQLException.class, () -> exec("INSERT INTO trp_cvv VALUES (-1)"));
        assertEquals("44000", e.getSQLState());
        exec("DROP VIEW trp_cvv");
        exec("DROP TABLE trp_cv CASCADE");
    }

    // ---- a rule reached through an updatable view ----

    @Test
    void aBaseTableRuleAlsoRunsWhenTheWriteArrivesThroughAView() throws Exception {
        exec("CREATE TABLE trp_rv (i int, j text)");
        exec("CREATE TABLE trp_rvlog (m text)");
        exec("CREATE VIEW trp_rvv AS SELECT i FROM trp_rv WHERE i > 0 WITH CHECK OPTION");
        exec("CREATE RULE trp_rvr AS ON INSERT TO trp_rv"
                + " DO ALSO INSERT INTO trp_rvlog VALUES (NEW.j)");
        try {
            // writing to the table runs the rule
            exec("INSERT INTO trp_rv VALUES (7, 'g')");
            assertEquals("g", scalar("SELECT string_agg(m, ',' ORDER BY m) FROM trp_rvlog"));
            // and so does writing through the view, which is rewritten onto the same table.
            // The view has no j, so NEW.j is null there rather than a name left unresolved.
            exec("INSERT INTO trp_rvv VALUES (8)");
            assertEquals("g", scalar("SELECT string_agg(m, ',' ORDER BY m) FROM trp_rvlog"));
            assertEquals("2", scalar("SELECT count(*)::text FROM trp_rvlog"));
            assertEquals("7,8", scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM trp_rv"));
        } finally {
            exec("DROP VIEW IF EXISTS trp_rvv");
            exec("DROP TABLE IF EXISTS trp_rv CASCADE");
            exec("DROP TABLE IF EXISTS trp_rvlog CASCADE");
        }
    }

    @Test
    void recursionIsDetectedThroughAViewToo() throws Exception {
        exec("CREATE TABLE trp_rr (i int, j text)");
        exec("CREATE TABLE trp_rrlog (m text)");
        exec("CREATE VIEW trp_rrv AS SELECT i FROM trp_rr WHERE i > 0 WITH CHECK OPTION");
        exec("CREATE RULE trp_rrself AS ON INSERT TO trp_rrlog"
                + " DO ALSO INSERT INTO trp_rrlog VALUES ('c')");
        exec("CREATE RULE trp_rrhop AS ON INSERT TO trp_rr"
                + " DO ALSO INSERT INTO trp_rrlog VALUES (NEW.j)");
        try {
            // the loop is one hop away from the table named, and two from the view
            assertRejected("42P17", "infinite recursion detected in rules for relation",
                    "INSERT INTO trp_rr VALUES (1, 'a')");
            assertRejected("42P17", "infinite recursion detected in rules for relation",
                    "INSERT INTO trp_rrv VALUES (5)");
            // nothing of the refused statement survives
            assertEquals("0", scalar("SELECT count(*)::text FROM trp_rrlog"));
            assertEquals("0", scalar("SELECT count(*)::text FROM trp_rr"));
        } finally {
            exec("DROP VIEW IF EXISTS trp_rrv");
            exec("DROP TABLE IF EXISTS trp_rr CASCADE");
            exec("DROP TABLE IF EXISTS trp_rrlog CASCADE");
        }
    }
}
