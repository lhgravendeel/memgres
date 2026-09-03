package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a membership grant may carry, and what a CALL reaches.
 *
 * <p>A membership has three options — ADMIN, INHERIT and SET — and each may be given as
 * {@code OPTION}, as {@code TRUE} or as {@code FALSE}, and revoked on its own leaving the
 * membership itself. A word that is none of the three is an option nobody has; a value that is
 * none of the three is a statement that will not parse.
 *
 * <p>A CALL reaches a procedure and nothing else. A name that belongs to a function is reported as
 * the routine it is rather than as a name nothing answers to, and a schema written in front of the
 * name is opened before anything inside it is looked for. A bare quoted argument has no type yet,
 * so it reaches a parameter of any type and is then read by that type — which is where a value the
 * parameter cannot hold is finally refused.
 */
class RoleGrantOptionsAndCallResolutionTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE ROLE zrg_a");
            s.execute("CREATE ROLE zrg_b");
            s.execute("CREATE PROCEDURE zrg_p(a int) LANGUAGE sql AS $$ SELECT 1 $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** Each of the three options may be given as OPTION, as TRUE or as FALSE. */
    @Test
    void theOptionsAMembershipCarries() {
        for (String with : new String[]{"ADMIN OPTION", "ADMIN TRUE", "ADMIN FALSE",
                "INHERIT TRUE", "INHERIT FALSE", "SET TRUE", "SET FALSE",
                "SET FALSE, INHERIT TRUE"}) {
            assertNull(stateOf("GRANT zrg_a TO zrg_b WITH " + with), with);
        }
    }

    /** A word that is not one of the three is an option nobody has. */
    @Test
    void aWordThatNamesNoOption() {
        assertTrue(messageOf("GRANT zrg_a TO zrg_b WITH NOSUCHOPTION TRUE")
                .contains("unrecognized role option \"nosuchoption\""));
        assertTrue(messageOf("GRANT zrg_a TO zrg_b WITH GRANT OPTION")
                .contains("unrecognized role option \"grant\""));
        // A value that is not one of the three never gets that far.
        assertTrue(messageOf("GRANT zrg_a TO zrg_b WITH INHERIT MAYBE")
                .contains("syntax error at or near \"MAYBE\""));
    }

    /** Any one of the three may be revoked, leaving the membership itself. */
    @Test
    void anOptionMayBeRevokedOnItsOwn() {
        assertNull(stateOf("GRANT zrg_a TO zrg_b WITH ADMIN OPTION"));
        assertNull(stateOf("REVOKE INHERIT OPTION FOR zrg_a FROM zrg_b"));
        assertNull(stateOf("REVOKE SET OPTION FOR zrg_a FROM zrg_b"));
        assertNull(stateOf("REVOKE ADMIN OPTION FOR zrg_a FROM zrg_b"));
        assertNull(stateOf("REVOKE zrg_a FROM zrg_b"));
    }

    /** A name that is a function is reported as the routine it is. */
    @Test
    void aCallReachesAProcedureAndNothingElse() {
        assertEquals("42809", stateOf("CALL count(1)"));
        assertTrue(messageOf("CALL count(1)").contains("count(integer) is not a procedure"));
        assertTrue(messageOf("CALL now()").contains("now() is not a procedure"));
        // A bare quoted argument has no type yet, which is what unknown means.
        assertTrue(messageOf("CALL upper('x')").contains("upper(unknown) is not a procedure"));
        assertEquals("42883", stateOf("CALL zrg_nosuch()"));
    }

    /** The schema is opened before the procedure inside it is looked for. */
    @Test
    void aSchemaWrittenInFrontOfTheName() {
        assertEquals("3F000", stateOf("CALL zrg_nosuchschema.zrg_p(1)"));
        assertNull(stateOf("CALL public.zrg_p(1)"));
    }

    /** An argument is read as the type of the parameter it is being passed to. */
    @Test
    void anArgumentIsReadAsItsParametersType() {
        assertNull(stateOf("CALL zrg_p(1)"));
        assertEquals("22P02", stateOf("CALL zrg_p('1.5')"));
        assertTrue(messageOf("CALL zrg_p('1.5')")
                .contains("invalid input syntax for type integer: \"1.5\""));
    }

    /** DEALLOCATE names one statement, and there is no IF EXISTS for it. */
    @Test
    void deallocateNamesOneStatement() {
        assertTrue(messageOf("DEALLOCATE IF EXISTS zrg_nosuch")
                .contains("syntax error at or near \"EXISTS\""));
        assertEquals("26000", stateOf("DEALLOCATE zrg_nosuch"));
    }
}
