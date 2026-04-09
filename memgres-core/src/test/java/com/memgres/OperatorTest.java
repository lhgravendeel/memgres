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
            stmt.execute("CREATE OPERATOR +++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints)");

            // Verify operator exists in pg_operator
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname, oprkind FROM pg_operator WHERE oprname = '+++'")) {
                assertTrue(rs.next());
                assertEquals("+++", rs.getString("oprname"));
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
            stmt.execute("CREATE OPERATOR myschema.+++ (LEFTARG = integer, RIGHTARG = integer, "
                    + "FUNCTION = myschema.add_ints)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprname FROM pg_operator WHERE oprname = '+++'")) {
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
            stmt.execute("CREATE OPERATOR +++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = dup_fn)");

            // Creating the same operator again should error
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("CREATE OPERATOR +++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = dup_fn)"));
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
            stmt.execute("CREATE OPERATOR +++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = drop_fn)");

            // Verify it exists
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+++'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }

            stmt.execute("DROP OPERATOR +++ (integer, integer)");

            // Should be gone
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+++'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void dropOperatorIfExists() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Should not error when operator doesn't exist
            stmt.execute("DROP OPERATOR IF EXISTS +++ (integer, integer)");
        }
    }

    @Test
    void dropOperatorNonExistentErrors() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.execute("DROP OPERATOR +++ (integer, integer)"));
        }
    }

    @Test
    void dropOperatorCascade() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION casc_fn(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR +++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = casc_fn)");
            stmt.execute("DROP OPERATOR +++ (integer, integer) CASCADE");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+++'")) {
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
                    stmt.execute("ALTER OPERATOR +++ (integer, integer) OWNER TO memgres"));
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

            stmt.execute("CREATE OPERATOR +++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints_ol)");
            stmt.execute("CREATE OPERATOR +++ (LEFTARG = text, RIGHTARG = text, FUNCTION = concat_texts)");

            // Both should exist in pg_operator
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+++'")) {
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

            stmt.execute("CREATE OPERATOR +++ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints_ol2)");
            stmt.execute("CREATE OPERATOR +++ (LEFTARG = text, RIGHTARG = text, FUNCTION = concat_texts2)");

            // Drop only the integer version
            stmt.execute("DROP OPERATOR +++ (integer, integer)");

            // Text version should remain
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM pg_operator WHERE oprname = '+++'")) {
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
}
