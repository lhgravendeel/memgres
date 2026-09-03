package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an operator family is made of, and what may be put into one.
 *
 * <p>A family holds its members by the place each one fills rather than by its own name: a number
 * saying which comparison or which support routine it stands for, and a pair of operand types
 * saying which operands it covers. The access method decides which numbers exist at all — btree
 * has five comparisons and six support routines, hash has one and three, and the others set no
 * bound. What is put into a place has to exist to be put there, and a place that was never filled
 * cannot be emptied.
 *
 * <p>The checks are made in PostgreSQL's own order, which is not the order they are written in:
 * an OWNER TO is settled by whether the role exists before the family is looked for at all.
 */
class OperatorFamilyMembershipTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OPERATOR FAMILY zofm_b USING btree");
            s.execute("CREATE OPERATOR FAMILY zofm_h USING hash");
            s.execute("CREATE OPERATOR FAMILY zofm_g USING gist");
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

    /** A family exists for one access method, and altering it says which. */
    @Test
    void aFamilyBelongsToOneAccessMethod() {
        assertEquals("42704",
                stateOf("ALTER OPERATOR FAMILY zofm_nofam USING btree ADD OPERATOR 1 = (int4, int4)"));
        assertEquals("42704",
                stateOf("ALTER OPERATOR FAMILY zofm_b USING hash ADD OPERATOR 1 = (int8, int8)"));
        // The method itself is judged first: a family cannot belong to one nothing implements.
        assertEquals("42704",
                stateOf("ALTER OPERATOR FAMILY zofm_b USING nosuchmethod ADD OPERATOR 1 = (int8, int8)"));
    }

    /** The role an OWNER TO names is settled before the family is looked for. */
    @Test
    void theNewOwnerIsSettledFirst() {
        assertEquals("42704", stateOf("ALTER OPERATOR FAMILY zofm_b USING btree OWNER TO zofm_norole"));
        assertTrue(messageOf("ALTER OPERATOR FAMILY zofm_nofam USING btree OWNER TO zofm_norole")
                .contains("role \"zofm_norole\" does not exist"));
        assertTrue(messageOf("ALTER OPERATOR CLASS zofm_nocls USING btree OWNER TO zofm_norole")
                .contains("role \"zofm_norole\" does not exist"));
        assertEquals("3F000", stateOf("ALTER OPERATOR FAMILY zofm_b USING btree SET SCHEMA zofm_nosch"));
    }

    /** Each access method has its own count of comparisons and of support routines. */
    @Test
    void whichNumbersAMethodHas() {
        assertTrue(messageOf("ALTER OPERATOR FAMILY zofm_b USING btree ADD OPERATOR 9 = (int8, int8)")
                .contains("invalid operator number 9, must be between 1 and 5"));
        assertEquals("42P17",
                stateOf("ALTER OPERATOR FAMILY zofm_b USING btree ADD OPERATOR 0 = (int8, int8)"));
        assertTrue(messageOf("ALTER OPERATOR FAMILY zofm_h USING hash ADD OPERATOR 2 = (int8, int8)")
                .contains("invalid operator number 2, must be between 1 and 1"));
        assertTrue(messageOf("ALTER OPERATOR FAMILY zofm_b USING btree"
                + " ADD FUNCTION 9 (int8, int8) btint8cmp(int8, int8)")
                .contains("invalid function number 9, must be between 1 and 6"));
        // A method that sets no bound takes whatever number is written.
        assertNull(stateOf("ALTER OPERATOR FAMILY zofm_g USING gist ADD OPERATOR 100 = (int8, int8)"));
    }

    /** What fills a place has to exist, and the place has to be a pair of operands. */
    @Test
    void whatMayFillAPlace() {
        assertTrue(messageOf("ALTER OPERATOR FAMILY zofm_b USING btree"
                + " ADD OPERATOR 3 ###?# (int8, int8)")
                .contains("operator does not exist: bigint ###?# bigint"));
        assertEquals("42704",
                stateOf("ALTER OPERATOR FAMILY zofm_b USING btree ADD OPERATOR 1 = (nosuchtype, int8)"));
        assertEquals("42601",
                stateOf("ALTER OPERATOR FAMILY zofm_b USING btree ADD OPERATOR 1 = (int8)"));
    }

    /** A place that was never filled cannot be emptied, and one emptied twice is empty once. */
    @Test
    void aPlaceIsFilledAndEmptiedOnce() {
        assertTrue(messageOf("ALTER OPERATOR FAMILY zofm_b USING btree DROP OPERATOR 5 (int8, int8)")
                .contains("operator 5(bigint,bigint) does not exist in operator family \"zofm_b\""));
        assertNull(stateOf("ALTER OPERATOR FAMILY zofm_b USING btree ADD OPERATOR 5 > (int8, int8)"));
        assertNull(stateOf("ALTER OPERATOR FAMILY zofm_b USING btree DROP OPERATOR 5 (int8, int8)"));
        assertEquals("42704",
                stateOf("ALTER OPERATOR FAMILY zofm_b USING btree DROP OPERATOR 5 (int8, int8)"));
        assertNull(stateOf("ALTER OPERATOR FAMILY zofm_b USING btree"
                + " ADD FUNCTION 1 (int8, int8) btint8cmp(int8, int8)"));
        assertNull(stateOf("ALTER OPERATOR FAMILY zofm_b USING btree DROP FUNCTION 1 (int8, int8)"));
        assertTrue(messageOf("ALTER OPERATOR FAMILY zofm_b USING btree DROP FUNCTION 1 (int8, int8)")
                .contains("function 1(bigint,bigint) does not exist in operator family \"zofm_b\""));
    }

    /** One statement may name several members, and each is put in or taken out in turn. */
    @Test
    void aListMayNameSeveralMembers() {
        assertNull(stateOf("ALTER OPERATOR FAMILY zofm_b USING btree"
                + " ADD OPERATOR 1 < (int8, int8), OPERATOR 2 <= (int8, int8)"));
        assertNull(stateOf("ALTER OPERATOR FAMILY zofm_b USING btree"
                + " DROP OPERATOR 1 (int8, int8), OPERATOR 2 (int8, int8)"));
        assertEquals("42704",
                stateOf("ALTER OPERATOR FAMILY zofm_b USING btree DROP OPERATOR 1 (int8, int8)"));
    }
}
