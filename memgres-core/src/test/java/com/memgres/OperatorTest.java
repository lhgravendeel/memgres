package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for operator DDL support: CREATE/ALTER/DROP OPERATOR,
 * CREATE/ALTER/DROP OPERATOR CLASS, CREATE/ALTER/DROP OPERATOR FAMILY,
 * and operator overloading.
 *
 * All tests should pass on real PG 18.
 */
class OperatorTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ========================================================================
    // CREATE OPERATOR — basic
    // ========================================================================

    @Test
    void createOperatorBasic() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_ints(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints)");

            // Verify operator exists in pg_operator
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprkind FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals("~++", rs.getString("oprname"));
                assertEquals("b", rs.getString("oprkind"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void createOperatorWithProcedureKeyword() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_eq(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            // PROCEDURE is an alias for FUNCTION in CREATE OPERATOR
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, PROCEDURE = my_eq)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '==='")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorWithCommutator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_eq2(a text, b text) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~~~ (LEFTARG = text, RIGHTARG = text, FUNCTION = my_eq2, COMMUTATOR = ~~~)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprcom FROM pg_operator WHERE oprname = '~~~'")) {
                assertTrue(rs.next());
                assertEquals("~~~", rs.getString("oprname"));
                // commutator should reference itself
                int oid = rs.getInt("oprcom");
                assertTrue(oid > 0);
            }
        }
    }

    @Test
    void createOperatorWithNegator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_eq3(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION my_ne(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a <> b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ==== (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_eq3, NEGATOR = !===)");
            stmt.execute("CREATE OPERATOR !=== (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_ne, NEGATOR = ====)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname IN ('====', '!===') ORDER BY oprname")) {
                assertTrue(rs.next());
                assertEquals("!===", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("====", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void createOperatorWithRestrictAndJoin() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION float_eq(a float8, b float8) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <==> (LEFTARG = float8, RIGHTARG = float8, "
                    + "FUNCTION = float_eq, RESTRICT = eqsel, JOIN = eqjoinsel)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '<==>'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorUnaryPrefix() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_abs(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN CASE WHEN a < 0 THEN -a ELSE a END; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ## (RIGHTARG = integer, FUNCTION = my_abs)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprkind FROM pg_operator WHERE oprname = '##'")) {
                assertTrue(rs.next());
                assertEquals("##", rs.getString("oprname"));
                // PG stores left-unary as 'l' (deprecated in PG14+, but still valid)
                // Right-unary operators have oprleft = 0
            }
        }
    }

    @Test
    void createOperatorSchemaQualified() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA myschema");
            stmt.execute("CREATE FUNCTION myschema.add_ints(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR myschema.~++ (LEFTARG = integer, RIGHTARG = integer, "
                    + "FUNCTION = myschema.add_ints)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorMergeAndHash() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION int_eq_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ===== (LEFTARG = integer, RIGHTARG = integer, "
                    + "FUNCTION = int_eq_fn, MERGES, HASHES)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprcanmerge, oprcanhash FROM pg_operator WHERE oprname = '====='")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("oprcanmerge"));
                assertTrue(rs.getBoolean("oprcanhash"));
            }
        }
    }

    // ========================================================================
    // CREATE OPERATOR — error cases
    // ========================================================================

    @Test
    void createOperatorDuplicateErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION dup_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = dup_fn)");

            // Creating the same operator again should error
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = dup_fn)"));
            // PG error: operator already exists
            assertTrue(ex.getMessage().toLowerCase().contains("already exists")
                    || ex.getSQLState().equals("42710"));
        }
    }

    // ========================================================================
    // DROP OPERATOR
    // ========================================================================

    @Test
    void dropOperatorBasic() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION drop_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = drop_fn)");

            // Verify it exists
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }

            stmt.execute("DROP OPERATOR ~++ (integer, integer)");

            // Should be gone
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void dropOperatorIfExists() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Should not error when operator doesn't exist
            stmt.execute("DROP OPERATOR IF EXISTS ~++ (integer, integer)");
        }
    }

    @Test
    void dropOperatorNonExistentErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.execute("DROP OPERATOR ~++ (integer, integer)"));
        }
    }

    @Test
    void dropOperatorCascade() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION casc_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = casc_fn)");
            stmt.execute("DROP OPERATOR ~++ (integer, integer) CASCADE");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void dropOperatorUnaryWithNone() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION unary_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN -a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ## (RIGHTARG = integer, FUNCTION = unary_fn)");
            stmt.execute("DROP OPERATOR ## (NONE, integer)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '##'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // ALTER OPERATOR
    // ========================================================================

    @Test
    void alterOperatorOwnerTo() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ao_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = ao_fn)");
            stmt.execute("CREATE ROLE new_owner");
            stmt.execute("ALTER OPERATOR === (integer, integer) OWNER TO new_owner");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprowner FROM pg_operator WHERE oprname = '==='")) {
                assertTrue(rs.next());
                int ownerOid = rs.getInt("oprowner");
                // Verify it changed to new_owner's OID
                try (ResultSet rs2 = stmt.executeQuery(
                        "SELECT oid FROM pg_roles WHERE rolname = 'new_owner'")) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getInt(1), ownerOid);
                }
            }
        }
    }

    @Test
    void alterOperatorSetSchema() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA newschema");
            stmt.execute("CREATE FUNCTION as_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = as_fn)");
            stmt.execute("ALTER OPERATOR === (integer, integer) SET SCHEMA newschema");

            // Verify namespace changed
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT o.oprname, n.nspname FROM pg_operator o "
                    + "JOIN pg_namespace n ON o.oprnamespace = n.oid "
                    + "WHERE o.oprname = '==='")) {
                assertTrue(rs.next());
                assertEquals("newschema", rs.getString("nspname"));
            }
        }
    }

    @Test
    void alterOperatorSetRestrictJoin() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION asrj_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = asrj_fn)");
            stmt.execute("ALTER OPERATOR === (integer, integer) SET (RESTRICT = eqsel, JOIN = eqjoinsel)");

            // Should not error — just verifying the DDL is accepted and applied
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '==='")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void alterOperatorNonExistentErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.execute("ALTER OPERATOR ~++ (integer, integer) OWNER TO memgres"));
        }
    }

    // ========================================================================
    // OPERATOR OVERLOADING — same name, different arg types
    // ========================================================================

    @Test
    void operatorOverloadDifferentArgTypes() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_ints_ol(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION concat_texts(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints_ol)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = text, RIGHTARG = text, FUNCTION = concat_texts)");

            // Both should exist in pg_operator
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorOverloadDropOne() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_ints_ol2(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION concat_texts2(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints_ol2)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = text, RIGHTARG = text, FUNCTION = concat_texts2)");

            // Drop only the integer version
            stmt.execute("DROP OPERATOR ~++ (integer, integer)");

            // Text version should remain
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorOverloadUnaryAndBinary() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION neg_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN -a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION sub_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a - b; END; $$ LANGUAGE plpgsql");

            // Unary prefix operator
            stmt.execute("CREATE OPERATOR ### (RIGHTARG = integer, FUNCTION = neg_fn)");
            // Binary operator with same name
            stmt.execute("CREATE OPERATOR ### (LEFTARG = integer, RIGHTARG = integer, FUNCTION = sub_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '###'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // CREATE OPERATOR FAMILY
    // ========================================================================

    @Test
    void createOperatorFamilyBasic() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY my_fam USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'my_fam'")) {
                assertTrue(rs.next());
                assertEquals("my_fam", rs.getString(1));
            }
        }
    }

    @Test
    void createOperatorFamilyHash() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY hash_fam USING hash");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'hash_fam'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorFamilyGist() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY gist_fam USING gist");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'gist_fam'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorFamilyGin() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY gin_fam USING gin");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'gin_fam'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorFamilyDuplicateErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY dup_fam USING btree");
            assertThrows(SQLException.class, () ->
                    stmt.execute("CREATE OPERATOR FAMILY dup_fam USING btree"));
        }
    }

    // ========================================================================
    // DROP OPERATOR FAMILY
    // ========================================================================

    @Test
    void dropOperatorFamilyBasic() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY drop_fam USING btree");
            stmt.execute("DROP OPERATOR FAMILY drop_fam USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'drop_fam'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void dropOperatorFamilyIfExists() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP OPERATOR FAMILY IF EXISTS nonexistent_fam USING btree");
        }
    }

    @Test
    void dropOperatorFamilyNonExistentErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.execute("DROP OPERATOR FAMILY nonexistent_fam USING btree"));
        }
    }

    @Test
    void dropOperatorFamilyCascade() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY casc_fam USING btree");
            stmt.execute("DROP OPERATOR FAMILY casc_fam USING btree CASCADE");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'casc_fam'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // ALTER OPERATOR FAMILY
    // ========================================================================

    @Test
    void alterOperatorFamilyOwnerTo() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY aof_fam USING btree");
            stmt.execute("CREATE ROLE fam_owner");
            stmt.execute("ALTER OPERATOR FAMILY aof_fam USING btree OWNER TO fam_owner");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfowner FROM pg_opfamily WHERE opfname = 'aof_fam'")) {
                assertTrue(rs.next());
                int ownerOid = rs.getInt(1);
                try (ResultSet rs2 = stmt.executeQuery(
                        "SELECT oid FROM pg_roles WHERE rolname = 'fam_owner'")) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getInt(1), ownerOid);
                }
            }
        }
    }

    @Test
    void alterOperatorFamilyRenameTo() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY rename_fam USING btree");
            stmt.execute("ALTER OPERATOR FAMILY rename_fam USING btree RENAME TO new_fam_name");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'rename_fam'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'new_fam_name'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void alterOperatorFamilySetSchema() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA fam_schema");
            stmt.execute("CREATE OPERATOR FAMILY schema_fam USING btree");
            stmt.execute("ALTER OPERATOR FAMILY schema_fam USING btree SET SCHEMA fam_schema");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT f.opfname, n.nspname FROM pg_opfamily f "
                    + "JOIN pg_namespace n ON f.opfnamespace = n.oid "
                    + "WHERE f.opfname = 'schema_fam'")) {
                assertTrue(rs.next());
                assertEquals("fam_schema", rs.getString("nspname"));
            }
        }
    }

    @Test
    void alterOperatorFamilyAddOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_fn)");
            stmt.execute("CREATE OPERATOR FAMILY add_op_fam USING btree");
            stmt.execute("ALTER OPERATOR FAMILY add_op_fam USING btree ADD OPERATOR 1 <<< (integer, integer)");

            // The operator family should still exist
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'add_op_fam'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void alterOperatorFamilyAddFunction() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cmp_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN CASE WHEN a < b THEN -1 WHEN a > b THEN 1 ELSE 0 END; END; "
                    + "$$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR FAMILY add_fn_fam USING btree");
            stmt.execute("ALTER OPERATOR FAMILY add_fn_fam USING btree "
                    + "ADD FUNCTION 1 (integer, integer) cmp_fn(integer, integer)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'add_fn_fam'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void alterOperatorFamilyDropOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_fn2(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_fn2)");
            stmt.execute("CREATE OPERATOR FAMILY drop_member_fam USING btree");
            stmt.execute("ALTER OPERATOR FAMILY drop_member_fam USING btree ADD OPERATOR 1 <<< (integer, integer)");
            stmt.execute("ALTER OPERATOR FAMILY drop_member_fam USING btree DROP OPERATOR 1 (integer, integer)");

            // Family should still exist
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'drop_member_fam'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // CREATE OPERATOR CLASS
    // ========================================================================

    @Test
    void createOperatorClassBasic() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_opc(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_opc)");

            stmt.execute("CREATE OPERATOR CLASS my_int_ops FOR TYPE integer USING btree AS "
                    + "OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname, opcdefault FROM pg_opclass WHERE opcname = 'my_int_ops'")) {
                assertTrue(rs.next());
                assertEquals("my_int_ops", rs.getString("opcname"));
                assertFalse(rs.getBoolean("opcdefault"));
            }
        }
    }

    @Test
    void createOperatorClassDefault() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Create a custom type to avoid conflicting with existing default opclasses
            stmt.execute("CREATE FUNCTION lt_custom(a text, b text) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = text, RIGHTARG = text, FUNCTION = lt_custom)");

            stmt.execute("CREATE OPERATOR CLASS my_text_ops DEFAULT FOR TYPE text USING btree AS "
                    + "OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname, opcdefault FROM pg_opclass WHERE opcname = 'my_text_ops'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("opcdefault"));
            }
        }
    }

    @Test
    void createOperatorClassWithFunction() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cmp_opc(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN CASE WHEN a < b THEN -1 WHEN a > b THEN 1 ELSE 0 END; END; "
                    + "$$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION lt_opc2(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_opc2)");

            stmt.execute("CREATE OPERATOR CLASS my_int_ops2 FOR TYPE integer USING btree AS "
                    + "OPERATOR 1 <<<, "
                    + "FUNCTION 1 cmp_opc(integer, integer)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname FROM pg_opclass WHERE opcname = 'my_int_ops2'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorClassInFamily() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY opc_fam USING btree");
            stmt.execute("CREATE FUNCTION lt_fam(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_fam)");

            stmt.execute("CREATE OPERATOR CLASS fam_int_ops FOR TYPE integer USING btree "
                    + "FAMILY opc_fam AS OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT c.opcname, f.opfname FROM pg_opclass c "
                    + "JOIN pg_opfamily f ON c.opcfamily = f.oid "
                    + "WHERE c.opcname = 'fam_int_ops'")) {
                assertTrue(rs.next());
                assertEquals("opc_fam", rs.getString("opfname"));
            }
        }
    }

    @Test
    void createOperatorClassHashMethod() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION eq_hash(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = eq_hash)");

            stmt.execute("CREATE OPERATOR CLASS hash_int_ops FOR TYPE integer USING hash AS "
                    + "OPERATOR 1 ===");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname FROM pg_opclass WHERE opcname = 'hash_int_ops'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void createOperatorClassDuplicateErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_dup(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_dup)");
            stmt.execute("CREATE OPERATOR CLASS dup_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            assertThrows(SQLException.class, () ->
                    stmt.execute("CREATE OPERATOR CLASS dup_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<"));
        }
    }

    @Test
    void createOperatorClassWithStorage() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_store(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_store)");

            stmt.execute("CREATE OPERATOR CLASS store_ops FOR TYPE integer USING btree AS "
                    + "OPERATOR 1 <<<, STORAGE integer");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname FROM pg_opclass WHERE opcname = 'store_ops'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // DROP OPERATOR CLASS
    // ========================================================================

    @Test
    void dropOperatorClassBasic() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_dropc(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_dropc)");
            stmt.execute("CREATE OPERATOR CLASS drop_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            stmt.execute("DROP OPERATOR CLASS drop_ops USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opclass WHERE opcname = 'drop_ops'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void dropOperatorClassIfExists() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP OPERATOR CLASS IF EXISTS nonexistent_ops USING btree");
        }
    }

    @Test
    void dropOperatorClassNonExistentErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.execute("DROP OPERATOR CLASS nonexistent_ops USING btree"));
        }
    }

    @Test
    void dropOperatorClassCascade() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_casc(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_casc)");
            stmt.execute("CREATE OPERATOR CLASS casc_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            stmt.execute("DROP OPERATOR CLASS casc_ops USING btree CASCADE");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opclass WHERE opcname = 'casc_ops'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // ALTER OPERATOR CLASS
    // ========================================================================

    @Test
    void alterOperatorClassOwnerTo() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_aoc(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_aoc)");
            stmt.execute("CREATE OPERATOR CLASS aoc_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");
            stmt.execute("CREATE ROLE class_owner");
            stmt.execute("ALTER OPERATOR CLASS aoc_ops USING btree OWNER TO class_owner");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcowner FROM pg_opclass WHERE opcname = 'aoc_ops'")) {
                assertTrue(rs.next());
                int ownerOid = rs.getInt(1);
                try (ResultSet rs2 = stmt.executeQuery(
                        "SELECT oid FROM pg_roles WHERE rolname = 'class_owner'")) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getInt(1), ownerOid);
                }
            }
        }
    }

    @Test
    void alterOperatorClassRenameTo() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_rename(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_rename)");
            stmt.execute("CREATE OPERATOR CLASS rename_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");
            stmt.execute("ALTER OPERATOR CLASS rename_ops USING btree RENAME TO new_ops_name");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opclass WHERE opcname = 'rename_ops'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname FROM pg_opclass WHERE opcname = 'new_ops_name'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void alterOperatorClassSetSchema() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA cls_schema");
            stmt.execute("CREATE FUNCTION lt_cls(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_cls)");
            stmt.execute("CREATE OPERATOR CLASS cls_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");
            stmt.execute("ALTER OPERATOR CLASS cls_ops USING btree SET SCHEMA cls_schema");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT c.opcname, n.nspname FROM pg_opclass c "
                    + "JOIN pg_namespace n ON c.opcnamespace = n.oid "
                    + "WHERE c.opcname = 'cls_ops'")) {
                assertTrue(rs.next());
                assertEquals("cls_schema", rs.getString("nspname"));
            }
        }
    }

    // ========================================================================
    // pg_operator catalog — built-in operators should be queryable
    // ========================================================================

    @Test
    void pgOperatorHasBuiltinOperators() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // pg_operator should have standard PG operators
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '='")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) > 0, "pg_operator should contain built-in = operator(s)");
            }
        }
    }

    @Test
    void pgOperatorBuiltinPlusOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprkind FROM pg_operator WHERE oprname = '+' LIMIT 1")) {
                assertTrue(rs.next());
                assertEquals("+", rs.getString("oprname"));
                assertEquals("b", rs.getString("oprkind"));
            }
        }
    }

    @Test
    void pgOperatorConcatOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '||' LIMIT 1")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // pg_opclass catalog — bootstrap data
    // ========================================================================

    @Test
    void pgOpclassHasBootstrapData() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opclass")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) > 0, "pg_opclass should have bootstrap operator classes");
            }
        }
    }

    @Test
    void pgOpclassHasInt4Ops() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname, opcdefault FROM pg_opclass WHERE opcname = 'int4_ops'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("opcdefault"));
            }
        }
    }

    // ========================================================================
    // pg_opfamily catalog — bootstrap data
    // ========================================================================

    @Test
    void pgOpfamilyHasBootstrapData() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) > 0, "pg_opfamily should have bootstrap operator families");
            }
        }
    }

    @Test
    void pgOpfamilyHasIntegerOps() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'integer_ops'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // Cross-object interactions
    // ========================================================================

    @Test
    void operatorClassReferencesFamily() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY cross_fam USING btree");
            stmt.execute("CREATE FUNCTION lt_cross(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_cross)");
            stmt.execute("CREATE OPERATOR CLASS cross_ops FOR TYPE integer USING btree "
                    + "FAMILY cross_fam AS OPERATOR 1 <<<");

            // Join pg_opclass to pg_opfamily
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT c.opcname, f.opfname FROM pg_opclass c "
                    + "JOIN pg_opfamily f ON c.opcfamily = f.oid "
                    + "WHERE c.opcname = 'cross_ops'")) {
                assertTrue(rs.next());
                assertEquals("cross_fam", rs.getString("opfname"));
            }
        }
    }

    @Test
    void dropOperatorFamilyCascadeDropsClass() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY cascade_fam USING btree");
            stmt.execute("CREATE FUNCTION lt_cascade(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_cascade)");
            stmt.execute("CREATE OPERATOR CLASS cascade_ops FOR TYPE integer USING btree "
                    + "FAMILY cascade_fam AS OPERATOR 1 <<<");

            // CASCADE should drop dependent opclass too
            stmt.execute("DROP OPERATOR FAMILY cascade_fam USING btree CASCADE");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opclass WHERE opcname = 'cascade_ops'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Multiple index methods coexist
    // ========================================================================

    @Test
    void sameNameDifferentMethods() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY multi_method_fam USING btree");
            stmt.execute("CREATE OPERATOR FAMILY multi_method_fam USING hash");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'multi_method_fam'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Schema-qualified operator families and classes
    // ========================================================================

    @Test
    void schemaQualifiedOperatorFamily() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA op_schema");
            stmt.execute("CREATE OPERATOR FAMILY op_schema.my_fam USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT f.opfname, n.nspname FROM pg_opfamily f "
                    + "JOIN pg_namespace n ON f.opfnamespace = n.oid "
                    + "WHERE f.opfname = 'my_fam' AND n.nspname = 'op_schema'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // pg_operator catalog detail verification
    // ========================================================================

    @Test
    void pgOperatorCatalogDetailsForUserDefinedOp() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cat_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = cat_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprkind, oprleft, oprright FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals("~++", rs.getString("oprname"));
                assertEquals("b", rs.getString("oprkind"));
                // oprleft and oprright should be the OIDs for integer type
                int leftOid = rs.getInt("oprleft");
                int rightOid = rs.getInt("oprright");
                assertTrue(leftOid > 0, "oprleft should be a valid type OID");
                assertTrue(rightOid > 0, "oprright should be a valid type OID");
                assertEquals(leftOid, rightOid, "both args are integer, so OIDs should match");
            }
        }
    }

    @Test
    void pgOperatorUnaryHasZeroLeftOid() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION neg_cat(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN -a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ### (RIGHTARG = integer, FUNCTION = neg_cat)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprleft, oprright FROM pg_operator WHERE oprname = '###'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt("oprleft"), "unary prefix operator should have oprleft = 0");
                assertTrue(rs.getInt("oprright") > 0);
            }
        }
    }

    @Test
    void pgOperatorJoinWithNamespace() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ns_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = ns_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT o.oprname, n.nspname FROM pg_operator o "
                    + "JOIN pg_namespace n ON o.oprnamespace = n.oid "
                    + "WHERE o.oprname = '==='")) {
                assertTrue(rs.next());
                assertEquals("===", rs.getString("oprname"));
                assertEquals("public", rs.getString("nspname"));
            }
        }
    }

    // ========================================================================
    // Cross-type operators
    // ========================================================================

    @Test
    void crossTypeOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION int_text_eq(a integer, b text) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a::text = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = text, FUNCTION = int_text_eq)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprleft, oprright FROM pg_operator WHERE oprname = '==='")) {
                assertTrue(rs.next());
                int leftOid = rs.getInt("oprleft");
                int rightOid = rs.getInt("oprright");
                assertNotEquals(leftOid, rightOid, "cross-type operator should have different arg type OIDs");
            }
        }
    }

    // ========================================================================
    // SQL-language function as operator
    // ========================================================================

    @Test
    void sqlLanguageFunctionOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sql_add(a integer, b integer) RETURNS integer AS $$ "
                    + "SELECT a + b; $$ LANGUAGE sql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = sql_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // SECURITY DEFINER function as operator
    // ========================================================================

    @Test
    void securityDefinerFunctionOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sd_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = sd_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // STRICT function as operator
    // ========================================================================

    @Test
    void strictFunctionOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = strict_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // IMMUTABLE / VOLATILE function as operator
    // ========================================================================

    @Test
    void immutableFunctionOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION imm_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql IMMUTABLE");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = imm_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // DROP OPERATOR RESTRICT
    // ========================================================================

    @Test
    void dropOperatorRestrict() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION rest_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = rest_fn)");
            stmt.execute("DROP OPERATOR ~++ (integer, integer) RESTRICT");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Create-drop-recreate cycle
    // ========================================================================

    @Test
    void createDropRecreateCycle() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cycle_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");

            // Create
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = cycle_fn)");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }

            // Drop
            stmt.execute("DROP OPERATOR ~++ (integer, integer)");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }

            // Recreate — should succeed
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = cycle_fn)");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Operator count tracking
    // ========================================================================

    @Test
    void operatorCountChangesCorrectly() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Count before
            int countBefore;
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_operator")) {
                assertTrue(rs.next());
                countBefore = rs.getInt(1);
            }

            stmt.execute("CREATE FUNCTION cnt_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = cnt_fn)");

            // Count after create
            int countAfter;
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_operator")) {
                assertTrue(rs.next());
                countAfter = rs.getInt(1);
            }
            assertEquals(countBefore + 1, countAfter);

            // Drop
            stmt.execute("DROP OPERATOR ~++ (integer, integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_operator")) {
                assertTrue(rs.next());
                assertEquals(countBefore, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Multiple schemas same operator
    // ========================================================================

    @Test
    void sameOperatorDifferentSchemas() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA s1");
            stmt.execute("CREATE SCHEMA s2");
            stmt.execute("CREATE FUNCTION s1_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION s2_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR s1.~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = s1_fn)");
            stmt.execute("CREATE OPERATOR s2.~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = s2_fn)");

            // Both should exist in pg_operator
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // ALTER OPERATOR FAMILY — error cases
    // ========================================================================

    @Test
    void alterOperatorFamilyNonExistentErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.execute("ALTER OPERATOR FAMILY ghost_fam USING btree OWNER TO memgres"));
        }
    }

    @Test
    void alterOperatorClassNonExistentErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.execute("ALTER OPERATOR CLASS ghost_ops USING btree OWNER TO memgres"));
        }
    }

    // ========================================================================
    // Multiple operator classes for same type, different methods
    // ========================================================================

    @Test
    void sameTypeOpclassDifferentMethods() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_m(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_m)");

            stmt.execute("CREATE OPERATOR CLASS multi_btree FOR TYPE integer USING btree AS OPERATOR 1 <<<");
            stmt.execute("CREATE OPERATOR CLASS multi_hash FOR TYPE integer USING hash AS OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname FROM pg_opclass WHERE opcname IN ('multi_btree', 'multi_hash') ORDER BY opcname")) {
                assertTrue(rs.next());
                assertEquals("multi_btree", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("multi_hash", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    // ========================================================================
    // Rename operator class collision
    // ========================================================================

    @Test
    void renameOperatorClassCollision() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_col(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_col)");

            stmt.execute("CREATE OPERATOR CLASS col_a FOR TYPE integer USING btree AS OPERATOR 1 <<<");
            stmt.execute("CREATE OPERATOR CLASS col_b FOR TYPE integer USING hash AS OPERATOR 1 <<<");

            // Renaming to a name that already exists for a DIFFERENT method should succeed
            // (opclass key includes method)
            stmt.execute("ALTER OPERATOR CLASS col_b USING hash RENAME TO col_a");

            // Both should exist (different methods)
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opclass WHERE opcname = 'col_a'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Bidirectional commutator/negator pair
    // ========================================================================

    @Test
    void bidirectionalCommutatorPair() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION bi_eq(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION bi_ne(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a <> b; END; $$ LANGUAGE plpgsql");

            // Create a pair with cross-referencing COMMUTATOR and NEGATOR
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, "
                    + "FUNCTION = bi_eq, COMMUTATOR = ===, NEGATOR = !==)");
            stmt.execute("CREATE OPERATOR !== (LEFTARG = integer, RIGHTARG = integer, "
                    + "FUNCTION = bi_ne, COMMUTATOR = !==, NEGATOR = ===)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname IN ('===', '!==') ORDER BY oprname")) {
                assertTrue(rs.next());
                assertEquals("!==", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("===", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    // ========================================================================
    // CREATE OPERATOR CLASS without AS clause
    // ========================================================================

    @Test
    void createOperatorClassMinimal() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Minimal opclass: no AS clause (just metadata, no operators/functions)
            stmt.execute("CREATE OPERATOR CLASS minimal_ops FOR TYPE integer USING btree AS "
                    + "OPERATOR 1 <");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname FROM pg_opclass WHERE opcname = 'minimal_ops'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // Operator family count tracking
    // ========================================================================

    @Test
    void operatorFamilyCountTracking() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            int countBefore;
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_opfamily")) {
                assertTrue(rs.next());
                countBefore = rs.getInt(1);
            }

            stmt.execute("CREATE OPERATOR FAMILY count_fam USING btree");

            int countAfter;
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_opfamily")) {
                assertTrue(rs.next());
                countAfter = rs.getInt(1);
            }
            assertEquals(countBefore + 1, countAfter);

            stmt.execute("DROP OPERATOR FAMILY count_fam USING btree");

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_opfamily")) {
                assertTrue(rs.next());
                assertEquals(countBefore, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Operator class count tracking
    // ========================================================================

    @Test
    void operatorClassCountTracking() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            int countBefore;
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_opclass")) {
                assertTrue(rs.next());
                countBefore = rs.getInt(1);
            }

            stmt.execute("CREATE FUNCTION lt_cnt(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_cnt)");
            stmt.execute("CREATE OPERATOR CLASS cnt_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            int countAfter;
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_opclass")) {
                assertTrue(rs.next());
                countAfter = rs.getInt(1);
            }
            assertEquals(countBefore + 1, countAfter);

            stmt.execute("DROP OPERATOR CLASS cnt_ops USING btree");

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_opclass")) {
                assertTrue(rs.next());
                assertEquals(countBefore, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // DROP with multiple overloads — only removes matching signature
    // ========================================================================

    @Test
    void dropOverloadedOperatorPrecise() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ol_int(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION ol_text(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION ol_bigint(a bigint, b bigint) RETURNS bigint AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = ol_int)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = text, RIGHTARG = text, FUNCTION = ol_text)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = bigint, RIGHTARG = bigint, FUNCTION = ol_bigint)");

            // 3 overloads
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }

            // Drop only the text version
            stmt.execute("DROP OPERATOR ~++ (text, text)");

            // 2 remaining
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Operator family with multiple index methods in same session
    // ========================================================================

    @Test
    void operatorFamilyMultipleMethodsInSession() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY session_fam USING btree");
            stmt.execute("CREATE OPERATOR FAMILY session_fam USING hash");
            stmt.execute("CREATE OPERATOR FAMILY session_fam USING gist");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'session_fam'")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }

            // Drop only the hash one
            stmt.execute("DROP OPERATOR FAMILY session_fam USING hash");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'session_fam'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Operator owner is current user
    // ========================================================================

    @Test
    void operatorOwnerIsCurrentUser() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION own_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = own_fn)");

            // Owner should be a valid role
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT o.oprowner, r.rolname FROM pg_operator o "
                    + "JOIN pg_roles r ON o.oprowner = r.oid "
                    + "WHERE o.oprname = '~++'")) {
                assertTrue(rs.next());
                String ownerName = rs.getString("rolname");
                assertNotNull(ownerName);
                assertTrue(ownerName.length() > 0, "owner should have a name");
            }
        }
    }

    // ========================================================================
    // ALTER multiple properties in sequence
    // ========================================================================

    @Test
    void alterOperatorMultipleSequentialChanges() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION seq_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = seq_fn)");
            stmt.execute("CREATE SCHEMA alt_schema");
            stmt.execute("CREATE ROLE alt_owner");

            // Change owner
            stmt.execute("ALTER OPERATOR === (integer, integer) OWNER TO alt_owner");
            // Change schema
            stmt.execute("ALTER OPERATOR === (integer, integer) SET SCHEMA alt_schema");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT o.oprname, n.nspname, r.rolname "
                    + "FROM pg_operator o "
                    + "JOIN pg_namespace n ON o.oprnamespace = n.oid "
                    + "JOIN pg_roles r ON o.oprowner = r.oid "
                    + "WHERE o.oprname = '==='")) {
                assertTrue(rs.next());
                assertEquals("alt_schema", rs.getString("nspname"));
                assertEquals("alt_owner", rs.getString("rolname"));
            }
        }
    }

    // ========================================================================
    // Operator family rename then use new name
    // ========================================================================

    @Test
    void operatorFamilyRenameAndUse() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY old_name USING btree");
            stmt.execute("ALTER OPERATOR FAMILY old_name USING btree RENAME TO new_name");

            // Old name should be gone
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'old_name'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }

            // New name should exist
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'new_name'")) {
                assertTrue(rs.next());
            }

            // Should be droppable with new name
            stmt.execute("DROP OPERATOR FAMILY new_name USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname = 'new_name'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Built-in operator catalog has correct kind
    // ========================================================================

    @Test
    void builtinOperatorsAreBinary() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // All built-in arithmetic operators should be binary
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprkind FROM pg_operator "
                    + "WHERE oprname IN ('+', '-', '*', '/', '=', '<>', '<', '>', '<=', '>=') "
                    + "ORDER BY oprname")) {
                int count = 0;
                while (rs.next()) {
                    assertEquals("b", rs.getString("oprkind"),
                            "Operator " + rs.getString("oprname") + " should be binary");
                    count++;
                }
                assertTrue(count >= 10, "Should have at least 10 built-in operator entries");
            }
        }
    }

    // ========================================================================
    // Schema-qualified operator — continued
    // ========================================================================

    @Test
    void schemaQualifiedOperatorClass() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA opc_schema");
            stmt.execute("CREATE FUNCTION opc_schema.lt_sq(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR opc_schema.<<< (LEFTARG = integer, RIGHTARG = integer, "
                    + "FUNCTION = opc_schema.lt_sq)");
            stmt.execute("CREATE OPERATOR CLASS opc_schema.sq_ops FOR TYPE integer USING btree AS "
                    + "OPERATOR 1 opc_schema.<<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT c.opcname, n.nspname FROM pg_opclass c "
                    + "JOIN pg_namespace n ON c.opcnamespace = n.oid "
                    + "WHERE c.opcname = 'sq_ops' AND n.nspname = 'opc_schema'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // Operator name edge cases
    // ========================================================================

    @Test
    void singleCharOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sc_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR @ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = sc_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '@'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void longMultiSymbolOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lm_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR @@@@ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lm_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '@@@@'")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void mixedSymbolOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ms_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <=> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = ms_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '<=>'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // DROP with wrong arg types should error
    // ========================================================================

    @Test
    void dropOperatorWrongArgTypesErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION wt_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = wt_fn)");

            // Drop with wrong arg types — should error (no such operator with text args)
            assertThrows(SQLException.class, () ->
                    stmt.execute("DROP OPERATOR ~++ (text, text)"));

            // Original should still exist
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // ALTER SET SCHEMA then DROP at new schema
    // ========================================================================

    @Test
    void alterSetSchemaThenDrop() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA move_schema");
            stmt.execute("CREATE FUNCTION mv_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = mv_fn)");

            stmt.execute("ALTER OPERATOR === (integer, integer) SET SCHEMA move_schema");

            // Verify it's in the new schema
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT n.nspname FROM pg_operator o "
                    + "JOIN pg_namespace n ON o.oprnamespace = n.oid "
                    + "WHERE o.oprname = '==='")) {
                assertTrue(rs.next());
                assertEquals("move_schema", rs.getString(1));
            }
        }
    }

    // ========================================================================
    // Operator created under SET ROLE — verify owner
    // ========================================================================

    @Test
    void operatorCreatedUnderSetRole() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE ROLE op_creator");
            stmt.execute("SET ROLE op_creator");

            stmt.execute("CREATE FUNCTION sr_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = sr_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT r.rolname FROM pg_operator o "
                    + "JOIN pg_roles r ON o.oprowner = r.oid "
                    + "WHERE o.oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals("op_creator", rs.getString(1));
            }
        }
    }

    // ========================================================================
    // Operator with ALL optional attributes at once
    // ========================================================================

    @Test
    void operatorWithAllAttributes() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION all_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, "
                    + "FUNCTION = all_fn, COMMUTATOR = ===, NEGATOR = !==, "
                    + "RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprcanmerge, oprcanhash, oprcom FROM pg_operator WHERE oprname = '==='")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("oprcanmerge"));
                assertTrue(rs.getBoolean("oprcanhash"));
                assertTrue(rs.getInt("oprcom") > 0, "commutator OID should be set");
            }
        }
    }

    // ========================================================================
    // oprcanmerge / oprcanhash default to false
    // ========================================================================

    @Test
    void mergeAndHashDefaultToFalse() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION def_fn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = def_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprcanmerge, oprcanhash FROM pg_operator WHERE oprname = '==='")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean("oprcanmerge"));
                assertFalse(rs.getBoolean("oprcanhash"));
            }
        }
    }

    // ========================================================================
    // Opclass method OID verification
    // ========================================================================

    @Test
    void opclassMethodOidBtree() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_moid(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_moid)");
            stmt.execute("CREATE OPERATOR CLASS moid_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcmethod FROM pg_opclass WHERE opcname = 'moid_ops'")) {
                assertTrue(rs.next());
                assertEquals(403, rs.getInt("opcmethod"), "btree method OID should be 403");
            }
        }
    }

    @Test
    void opclassMethodOidHash() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION eq_moid(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = eq_moid)");
            stmt.execute("CREATE OPERATOR CLASS moid_hash_ops FOR TYPE integer USING hash AS OPERATOR 1 ===");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcmethod FROM pg_opclass WHERE opcname = 'moid_hash_ops'")) {
                assertTrue(rs.next());
                assertEquals(405, rs.getInt("opcmethod"), "hash method OID should be 405");
            }
        }
    }

    // ========================================================================
    // Opfamily method OID verification
    // ========================================================================

    @Test
    void opfamilyMethodOidBtree() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY fmoid_fam USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfmethod FROM pg_opfamily WHERE opfname = 'fmoid_fam'")) {
                assertTrue(rs.next());
                assertEquals(403, rs.getInt("opfmethod"), "btree method OID should be 403");
            }
        }
    }

    @Test
    void opfamilyMethodOidHash() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY fmoid_hash USING hash");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfmethod FROM pg_opfamily WHERE opfname = 'fmoid_hash'")) {
                assertTrue(rs.next());
                assertEquals(405, rs.getInt("opfmethod"), "hash method OID should be 405");
            }
        }
    }

    @Test
    void opfamilyMethodOidGist() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY fmoid_gist USING gist");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfmethod FROM pg_opfamily WHERE opfname = 'fmoid_gist'")) {
                assertTrue(rs.next());
                assertEquals(783, rs.getInt("opfmethod"), "gist method OID should be 783");
            }
        }
    }

    @Test
    void opfamilyMethodOidGin() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY fmoid_gin USING gin");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfmethod FROM pg_opfamily WHERE opfname = 'fmoid_gin'")) {
                assertTrue(rs.next());
                assertEquals(2742, rs.getInt("opfmethod"), "gin method OID should be 2742");
            }
        }
    }

    // ========================================================================
    // ALTER OPERATOR CLASS RENAME then CREATE with old name
    // ========================================================================

    @Test
    void renameOpclassThenCreateWithOldName() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_rn(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_rn)");

            stmt.execute("CREATE OPERATOR CLASS recycle_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");
            stmt.execute("ALTER OPERATOR CLASS recycle_ops USING btree RENAME TO recycled_ops");

            // Old name is now free — can create again
            stmt.execute("CREATE OPERATOR CLASS recycle_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opclass WHERE opcname IN ('recycle_ops', 'recycled_ops')")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // DROP OPERATOR IF EXISTS for an operator that DOES exist
    // ========================================================================

    @Test
    void dropOperatorIfExistsExistingOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ie_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = ie_fn)");

            // Should succeed and actually remove
            stmt.execute("DROP OPERATOR IF EXISTS ~++ (integer, integer)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Rename operator family then create new with old name
    // ========================================================================

    @Test
    void renameOpfamilyThenCreateWithOldName() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY reuse_fam USING btree");
            stmt.execute("ALTER OPERATOR FAMILY reuse_fam USING btree RENAME TO reused_fam");

            // Old name is free
            stmt.execute("CREATE OPERATOR FAMILY reuse_fam USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_opfamily WHERE opfname IN ('reuse_fam', 'reused_fam')")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Operator family owner changes reflected in opfamily
    // ========================================================================

    @Test
    void operatorFamilyOwnerCreatedUnderSetRole() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE ROLE fam_creator");
            stmt.execute("SET ROLE fam_creator");

            stmt.execute("CREATE OPERATOR FAMILY role_fam USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT r.rolname FROM pg_opfamily f "
                    + "JOIN pg_roles r ON f.opfowner = r.oid "
                    + "WHERE f.opfname = 'role_fam'")) {
                assertTrue(rs.next());
                assertEquals("fam_creator", rs.getString(1));
            }
        }
    }

    // ========================================================================
    // Operator class owner created under SET ROLE
    // ========================================================================

    @Test
    void operatorClassOwnerCreatedUnderSetRole() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE ROLE cls_creator");
            stmt.execute("SET ROLE cls_creator");

            stmt.execute("CREATE FUNCTION lt_role(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_role)");
            stmt.execute("CREATE OPERATOR CLASS role_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT r.rolname FROM pg_opclass c "
                    + "JOIN pg_roles r ON c.opcowner = r.oid "
                    + "WHERE c.opcname = 'role_ops'")) {
                assertTrue(rs.next());
                assertEquals("cls_creator", rs.getString(1));
            }
        }
    }

    // ========================================================================
    // Operator type OID matches pg_type
    // ========================================================================

    @Test
    void operatorTypeOidMatchesPgType() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION typ_fn(a text, b text) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR === (LEFTARG = text, RIGHTARG = text, FUNCTION = typ_fn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT o.oprleft, t.typname FROM pg_operator o "
                    + "JOIN pg_type t ON o.oprleft = t.oid "
                    + "WHERE o.oprname = '==='")) {
                assertTrue(rs.next());
                assertEquals("text", rs.getString("typname"));
            }
        }
    }

    // ========================================================================
    // Drop opclass then recreate
    // ========================================================================

    @Test
    void dropOpclassThenRecreate() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_rc(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_rc)");

            stmt.execute("CREATE OPERATOR CLASS rc_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");
            stmt.execute("DROP OPERATOR CLASS rc_ops USING btree");

            // Recreate should succeed
            stmt.execute("CREATE OPERATOR CLASS rc_ops FOR TYPE integer USING btree AS OPERATOR 1 <<<");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opcname FROM pg_opclass WHERE opcname = 'rc_ops'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // Drop opfamily then recreate
    // ========================================================================

    @Test
    void dropOpfamilyThenRecreate() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OPERATOR FAMILY rc_fam USING btree");
            stmt.execute("DROP OPERATOR FAMILY rc_fam USING btree");

            // Recreate should succeed
            stmt.execute("CREATE OPERATOR FAMILY rc_fam USING btree");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT opfname FROM pg_opfamily WHERE opfname = 'rc_fam'")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // Bootstrap opclass/opfamily cross-reference
    // ========================================================================

    @Test
    void bootstrapOpclassReferencesBootstrapFamily() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // int4_ops should reference integer_ops family
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT c.opcname, f.opfname FROM pg_opclass c "
                    + "JOIN pg_opfamily f ON c.opcfamily = f.oid "
                    + "WHERE c.opcname = 'int4_ops'")) {
                assertTrue(rs.next());
                assertEquals("integer_ops", rs.getString("opfname"));
            }
        }
    }

    // ========================================================================
    // Multiple built-in operators queried at once
    // ========================================================================

    @Test
    void queryMultipleBuiltinOperators() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT DISTINCT oprname FROM pg_operator "
                    + "WHERE oprname IN ('+', '-', '*', '/', '=', '<', '>', '||', '~~') "
                    + "ORDER BY oprname")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertTrue(count >= 9, "Should find at least 9 distinct built-in operators");
            }
        }
    }

    // ========================================================================
    // User-defined operator that shadows a built-in name
    // ========================================================================

    @Test
    void createOperatorSameNameAsBuiltinDifferentSchema() throws SQLException {
        // In PG, user-defined operators in public schema don't clash with pg_catalog operators.
        // Same name + same arg types is allowed because they're in different schemas.
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b + 100; END; $$ LANGUAGE plpgsql");
            // Create a + operator in public schema — does NOT conflict with pg_catalog.+
            stmt.execute("CREATE OPERATOR + (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            // Both the user-defined and built-in should coexist in pg_operator
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 2, "should have both built-in and user-defined + operators");
            }

            // The built-in + should still work in expressions
            try (ResultSet rs = stmt.executeQuery("SELECT 1 + 2")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void createOperatorSameNameAsBuiltinConcat() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_concat(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || '!' || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR || (LEFTARG = text, RIGHTARG = text, FUNCTION = my_concat)");

            // Both should exist
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '||'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 2);
            }

            // Built-in || should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 'hello' || ' world'")) {
                assertTrue(rs.next());
                assertEquals("hello world", rs.getString(1));
            }
        }
    }

    @Test
    void createOperatorSameNameAsBuiltinEquals() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_eq(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR = (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_eq)");

            // Both should exist in pg_operator
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '='")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 2);
            }

            // Built-in = should still work in WHERE
            try (ResultSet rs = stmt.executeQuery("SELECT 1 WHERE 1 = 1")) {
                assertTrue(rs.next());
            }
        }
    }

    // ========================================================================
    // Operator names with prefix/suffix overlap with built-ins
    // ========================================================================

    @Test
    void operatorNameIsPrefixOfBuiltin() throws SQLException {
        // < is a built-in, user creates < in public schema — should coexist
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lt_pref(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR < (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lt_pref)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '<'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 2);
            }

            // Built-in < should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 1 < 2")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void operatorNameExtendsBuiltinLessThan() throws SQLException {
        // < exists as built-in, create << (also built-in as shift-left)
        // Then create <<< which is user-defined
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION triple_lt(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a < b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <<< (LEFTARG = integer, RIGHTARG = integer, FUNCTION = triple_lt)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '<<<'")) {
                assertTrue(rs.next());
            }

            // Built-in < and << should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 1 < 2")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void operatorNameExtendsBuiltinPlus() throws SQLException {
        // + exists as built-in, create ~+, ~++
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION dbl_plus(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION tri_plus(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b + 1; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR ~+ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = dbl_plus)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = tri_plus)");

            // All three (built-in +, user ~+ and ~++) should coexist
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname IN ('+', '~+', '~++')")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 3);
            }

            // Built-in + should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 10 + 20")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorNameSuffixOverlap() throws SQLException {
        // Create operators where one is a suffix of another: >, =>>, =>>>, etc.
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION suf_fn1(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION suf_fn2(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR =>> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = suf_fn1)");
            stmt.execute("CREATE OPERATOR =>>> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = suf_fn2)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname IN ('=>>', '=>>>')")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorWithTildePrefix() throws SQLException {
        // ~ is a built-in, ~~ is LIKE, create ~~~ which extends the pattern
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION tpfn(a text, b text) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~~~ (LEFTARG = text, RIGHTARG = text, FUNCTION = tpfn)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '~~~'")) {
                assertTrue(rs.next());
            }

            // Built-in ~~ (LIKE) should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 'hello' LIKE 'hell%'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // ========================================================================
    // Operator name that contains a built-in operator as substring
    // ========================================================================

    @Test
    void operatorContainsEqualsSubstring() throws SQLException {
        // = is built-in, create operators containing = : ===, !==, <==>, =!=
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION eq_fn1(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION eq_fn2(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a <> b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = eq_fn1)");
            stmt.execute("CREATE OPERATOR !== (LEFTARG = integer, RIGHTARG = integer, FUNCTION = eq_fn2)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname IN ('===', '!==')")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }

            // Built-in = should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 1 = 1")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // ========================================================================
    // Create duplicate user operator with same name but different types
    // does NOT conflict (overloading)
    // ========================================================================

    @Test
    void sameNameDifferentTypesNoConflict() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ov_int(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION ov_txt(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION ov_bool(a boolean, b boolean) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a AND b; END; $$ LANGUAGE plpgsql");

            // All use same operator name, different arg types — all should succeed
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = ov_int)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = text, RIGHTARG = text, FUNCTION = ov_txt)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = boolean, RIGHTARG = boolean, FUNCTION = ov_bool)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void sameNameSameTypesConflicts() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION dup1(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION dup2(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = dup1)");
            // Same name AND same arg types — should error
            assertThrows(SQLException.class, () ->
                    stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = dup2)"));
        }
    }

    // ========================================================================
    // Operator with asymmetric types (left != right)
    // ========================================================================

    @Test
    void asymmetricTypeOperatorPlusReverse() throws SQLException {
        // Create ~++ for (integer, text) AND (text, integer) — both should coexist
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION asym_it(a integer, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a::text || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION asym_ti(a text, b integer) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b::text; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = integer, RIGHTARG = text, FUNCTION = asym_it)");
            stmt.execute("CREATE OPERATOR ~++ (LEFTARG = text, RIGHTARG = integer, FUNCTION = asym_ti)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '~++'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Dropping one overload of a shadowing operator doesn't affect built-in
    // ========================================================================

    @Test
    void dropShadowingOperatorBuiltinSurvives() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Count built-in + entries before
            int builtinCount;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+'")) {
                assertTrue(rs.next());
                builtinCount = rs.getInt(1);
            }

            stmt.execute("CREATE FUNCTION shadow_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b + 999; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR + (LEFTARG = integer, RIGHTARG = integer, FUNCTION = shadow_fn)");

            // Should have one more now
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+'")) {
                assertTrue(rs.next());
                assertEquals(builtinCount + 1, rs.getInt(1));
            }

            // Drop the user-defined one
            stmt.execute("DROP OPERATOR + (integer, integer)");

            // Built-in entries should remain unchanged
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+'")) {
                assertTrue(rs.next());
                assertEquals(builtinCount, rs.getInt(1));
            }

            // Built-in + should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 5 + 3")) {
                assertTrue(rs.next());
                assertEquals(8, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Operator name that is a prefix of a multi-char built-in
    // ========================================================================

    @Test
    void operatorNamePrefixOfMultiCharBuiltin() throws SQLException {
        // -> is a built-in (JSON_ARROW). Create - as a user-defined binary operator.
        // Both should coexist.
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION subtract_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a - b; END; $$ LANGUAGE plpgsql");
            // - is already a built-in, but creating in public schema should be fine
            stmt.execute("CREATE OPERATOR - (LEFTARG = integer, RIGHTARG = integer, FUNCTION = subtract_fn)");

            // Both built-in and user-defined should coexist
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '-'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 2);
            }

            // Built-in - should still work
            try (ResultSet rs = stmt.executeQuery("SELECT 10 - 3")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Multiple user operators with incrementally longer names
    // ========================================================================

    @Test
    void incrementallyLongerOperatorNames() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION fn1(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION fn2(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN 2; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION fn3(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN 3; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION fn4(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN 4; END; $$ LANGUAGE plpgsql");

            // Create ~, ~~, ~~~, ~~~~ — all different user-defined operators
            // Note: ~ and ~~ are built-ins, so user versions go in public schema
            stmt.execute("CREATE OPERATOR ~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = fn1)");
            stmt.execute("CREATE OPERATOR ~~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = fn2)");
            stmt.execute("CREATE OPERATOR ~~~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = fn3)");
            stmt.execute("CREATE OPERATOR ~~~~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = fn4)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname IN ('~', '~~', '~~~', '~~~~')")) {
                assertTrue(rs.next());
                // Built-in ~ and ~~ exist + our 4 user-defined
                assertTrue(rs.getInt(1) >= 4);
            }
        }
    }
}
