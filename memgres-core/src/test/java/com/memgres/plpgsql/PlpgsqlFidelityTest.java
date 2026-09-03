package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PL/pgSQL fidelity tests covering PostgreSQL semantics for:
 * <ul>
 *   <li>BEFORE row triggers returning NULL (skip the row)</li>
 *   <li>AFTER trigger return values being ignored</li>
 *   <li>FOUND after plain DML, PERFORM, FOR loops, RETURN QUERY (and EXECUTE not changing it)</li>
 *   <li>Variable vs column name conflicts (42702, plpgsql.variable_conflict = error)</li>
 *   <li>UPDATE SET targets never being substituted</li>
 *   <li>EXECUTE ... USING parameter splicing (quoting, $10 vs $1)</li>
 *   <li>Non-STRICT SELECT/EXECUTE INTO with zero rows setting targets to NULL</li>
 *   <li>FOR loop variables living in an implicit inner block</li>
 *   <li>FOR ... BY 0 raising 22023</li>
 * </ul>
 */
class PlpgsqlFidelityTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query1(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    // =========================================================================
    // 1. BEFORE row trigger RETURN NULL skips the row
    // =========================================================================

    @Test
    void before_insert_return_null_skips_row() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bi_skip (id int PRIMARY KEY, val int)");
            s.execute("""
                CREATE OR REPLACE FUNCTION bi_skip_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.val < 0 THEN RETURN NULL; END IF;
                  RETURN NEW;
                END;
                $$""");
            s.execute("CREATE TRIGGER bi_skip_trg BEFORE INSERT ON bi_skip FOR EACH ROW EXECUTE FUNCTION bi_skip_fn()");

            // Skipped row: affected count is 0
            int count = s.executeUpdate("INSERT INTO bi_skip VALUES (1, -5)");
            assertEquals(0, count, "skipped row must not be counted");
            assertEquals("0", query1(s, "SELECT count(*) FROM bi_skip"));

            // Mixed batch: only the non-skipped row lands and is counted
            count = s.executeUpdate("INSERT INTO bi_skip VALUES (2, -1), (3, 30)");
            assertEquals(1, count);
            assertEquals("3", query1(s, "SELECT id FROM bi_skip"));

            // RETURNING excludes skipped rows
            try (ResultSet rs = s.executeQuery("INSERT INTO bi_skip VALUES (4, -4) RETURNING id")) {
                assertFalse(rs.next(), "RETURNING must be empty for a skipped row");
            }
            try (ResultSet rs = s.executeQuery("INSERT INTO bi_skip VALUES (5, -9), (6, 60) RETURNING id")) {
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
                assertFalse(rs.next());
            }

            s.execute("DROP TABLE bi_skip CASCADE");
            s.execute("DROP FUNCTION IF EXISTS bi_skip_fn()");
        }
    }

    @Test
    void before_update_return_null_leaves_row_untouched() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bu_skip (id int PRIMARY KEY, val int)");
            s.execute("INSERT INTO bu_skip VALUES (1, 10), (2, 20)");
            s.execute("""
                CREATE OR REPLACE FUNCTION bu_skip_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.val > 100 THEN RETURN NULL; END IF;
                  RETURN NEW;
                END;
                $$""");
            s.execute("CREATE TRIGGER bu_skip_trg BEFORE UPDATE ON bu_skip FOR EACH ROW EXECUTE FUNCTION bu_skip_fn()");

            // Blocked update: count 0, row unchanged
            int count = s.executeUpdate("UPDATE bu_skip SET val = 999 WHERE id = 1");
            assertEquals(0, count, "skipped update must not be counted");
            assertEquals("10", query1(s, "SELECT val FROM bu_skip WHERE id = 1"));

            // RETURNING excludes skipped rows; allowed update still goes through
            try (ResultSet rs = s.executeQuery("UPDATE bu_skip SET val = 500 WHERE id = 2 RETURNING val")) {
                assertFalse(rs.next(), "RETURNING must be empty when the row was skipped");
            }
            count = s.executeUpdate("UPDATE bu_skip SET val = 55 WHERE id = 2");
            assertEquals(1, count);
            assertEquals("55", query1(s, "SELECT val FROM bu_skip WHERE id = 2"));

            s.execute("DROP TABLE bu_skip CASCADE");
            s.execute("DROP FUNCTION IF EXISTS bu_skip_fn()");
        }
    }

    @Test
    void before_delete_return_null_keeps_row() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bd_skip (id int PRIMARY KEY, protected boolean)");
            s.execute("INSERT INTO bd_skip VALUES (1, true), (2, false)");
            s.execute("""
                CREATE OR REPLACE FUNCTION bd_skip_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF OLD.protected THEN RETURN NULL; END IF;
                  RETURN OLD;
                END;
                $$""");
            s.execute("CREATE TRIGGER bd_skip_trg BEFORE DELETE ON bd_skip FOR EACH ROW EXECUTE FUNCTION bd_skip_fn()");

            // Protected row survives, count 0
            int count = s.executeUpdate("DELETE FROM bd_skip WHERE id = 1");
            assertEquals(0, count, "skipped delete must not be counted");
            assertEquals("2", query1(s, "SELECT count(*) FROM bd_skip"));

            // Unprotected row is deleted and counted
            count = s.executeUpdate("DELETE FROM bd_skip WHERE id = 2");
            assertEquals(1, count);
            assertEquals("1", query1(s, "SELECT count(*) FROM bd_skip"));

            s.execute("DROP TABLE bd_skip CASCADE");
            s.execute("DROP FUNCTION IF EXISTS bd_skip_fn()");
        }
    }

    // =========================================================================
    // 2. AFTER trigger return value is ignored
    // =========================================================================

    @Test
    void after_trigger_return_null_is_ignored() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ai_t (id int PRIMARY KEY, val int)");
            s.execute("CREATE TABLE ai_log (id serial, msg text)");
            s.execute("""
                CREATE OR REPLACE FUNCTION ai_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ai_log(msg) VALUES ('fired');
                  RETURN NULL;
                END;
                $$""");
            s.execute("CREATE TRIGGER ai_trg AFTER INSERT ON ai_t FOR EACH ROW EXECUTE FUNCTION ai_fn()");

            int count = s.executeUpdate("INSERT INTO ai_t VALUES (1, 10)");
            assertEquals(1, count, "AFTER trigger RETURN NULL must not affect the insert");
            assertEquals("1", query1(s, "SELECT count(*) FROM ai_t"));
            assertEquals("1", query1(s, "SELECT count(*) FROM ai_log"));

            s.execute("DROP TABLE ai_t CASCADE");
            s.execute("DROP TABLE ai_log");
            s.execute("DROP FUNCTION IF EXISTS ai_fn()");
        }
    }

    @Test
    void after_trigger_new_modifications_are_ignored() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE au_t (id int PRIMARY KEY, val int)");
            s.execute("INSERT INTO au_t VALUES (1, 10)");
            s.execute("""
                CREATE OR REPLACE FUNCTION au_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  NEW.val := 12345;
                  RETURN NEW;
                END;
                $$""");
            s.execute("CREATE TRIGGER au_trg AFTER UPDATE ON au_t FOR EACH ROW EXECUTE FUNCTION au_fn()");

            s.execute("UPDATE au_t SET val = 20 WHERE id = 1");
            assertEquals("20", query1(s, "SELECT val FROM au_t WHERE id = 1"));

            s.execute("DROP TABLE au_t CASCADE");
            s.execute("DROP FUNCTION IF EXISTS au_fn()");
        }
    }

    // =========================================================================
    // 3. FOUND semantics
    // =========================================================================

    @Test
    void found_after_update_insert_delete() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE f_t (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO f_t VALUES (1, 'a')");
            s.execute("""
                CREATE OR REPLACE FUNCTION f_upd(p int) RETURNS boolean LANGUAGE plpgsql AS $$
                BEGIN UPDATE f_t SET v = 'x' WHERE id = p; RETURN FOUND; END;
                $$""");
            s.execute("""
                CREATE OR REPLACE FUNCTION f_ins(p int) RETURNS boolean LANGUAGE plpgsql AS $$
                BEGIN INSERT INTO f_t VALUES (p, 'new'); RETURN FOUND; END;
                $$""");
            s.execute("""
                CREATE OR REPLACE FUNCTION f_del(p int) RETURNS boolean LANGUAGE plpgsql AS $$
                BEGIN DELETE FROM f_t WHERE id = p; RETURN FOUND; END;
                $$""");

            assertEquals("true", query1(s, "SELECT f_upd(1)::text"), "FOUND after UPDATE hit");
            assertEquals("false", query1(s, "SELECT f_upd(999)::text"), "FOUND after UPDATE miss");
            assertEquals("true", query1(s, "SELECT f_ins(2)::text"), "FOUND after INSERT");
            assertEquals("true", query1(s, "SELECT f_del(2)::text"), "FOUND after DELETE hit");
            assertEquals("false", query1(s, "SELECT f_del(999)::text"), "FOUND after DELETE miss");

            s.execute("DROP FUNCTION f_upd(int)");
            s.execute("DROP FUNCTION f_ins(int)");
            s.execute("DROP FUNCTION f_del(int)");
            s.execute("DROP TABLE f_t CASCADE");
        }
    }

    @Test
    void found_after_perform_and_for_loop() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE fp_t (id int PRIMARY KEY)");
            s.execute("INSERT INTO fp_t VALUES (1)");
            s.execute("""
                CREATE OR REPLACE FUNCTION fp_perform(p int) RETURNS boolean LANGUAGE plpgsql AS $$
                BEGIN PERFORM 1 FROM fp_t WHERE id = p; RETURN FOUND; END;
                $$""");
            s.execute("""
                CREATE OR REPLACE FUNCTION fp_intfor() RETURNS boolean LANGUAGE plpgsql AS $$
                BEGIN FOR i IN 1..3 LOOP NULL; END LOOP; RETURN FOUND; END;
                $$""");
            s.execute("""
                CREATE OR REPLACE FUNCTION fp_queryfor(p int) RETURNS boolean LANGUAGE plpgsql AS $$
                DECLARE r record;
                BEGIN
                  FOR r IN SELECT id FROM fp_t WHERE id = p LOOP NULL; END LOOP;
                  RETURN FOUND;
                END;
                $$""");

            assertEquals("true", query1(s, "SELECT fp_perform(1)::text"), "FOUND after PERFORM hit");
            assertEquals("false", query1(s, "SELECT fp_perform(999)::text"), "FOUND after PERFORM miss");
            assertEquals("true", query1(s, "SELECT fp_intfor()::text"), "FOUND after integer FOR that iterated");
            assertEquals("true", query1(s, "SELECT fp_queryfor(1)::text"), "FOUND after query FOR that iterated");
            assertEquals("false", query1(s, "SELECT fp_queryfor(999)::text"), "FOUND after query FOR with no rows");

            s.execute("DROP FUNCTION fp_perform(int)");
            s.execute("DROP FUNCTION fp_intfor()");
            s.execute("DROP FUNCTION fp_queryfor(int)");
            s.execute("DROP TABLE fp_t CASCADE");
        }
    }

    @Test
    void found_is_not_changed_by_execute() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE fe_t (id int PRIMARY KEY)");
            s.execute("INSERT INTO fe_t VALUES (1)");
            s.execute("""
                CREATE OR REPLACE FUNCTION fe_fn() RETURNS boolean LANGUAGE plpgsql AS $$
                BEGIN
                  PERFORM 1 FROM fe_t WHERE id = 1;  -- sets FOUND = true
                  EXECUTE 'SELECT 1 WHERE 1 = 0';    -- must NOT change FOUND
                  RETURN FOUND;
                END;
                $$""");
            assertEquals("true", query1(s, "SELECT fe_fn()::text"));
            s.execute("DROP FUNCTION fe_fn()");
            s.execute("DROP TABLE fe_t CASCADE");
        }
    }

    @Test
    void upsert_idiom_update_if_not_found_insert() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ups_t (k int PRIMARY KEY, v text)");
            s.execute("""
                CREATE OR REPLACE FUNCTION ups_fn(p_k int, p_v text) RETURNS text LANGUAGE plpgsql AS $$
                BEGIN
                  UPDATE ups_t SET v = p_v WHERE k = p_k;
                  IF NOT FOUND THEN
                    INSERT INTO ups_t VALUES (p_k, p_v);
                    RETURN 'inserted';
                  END IF;
                  RETURN 'updated';
                END;
                $$""");

            assertEquals("inserted", query1(s, "SELECT ups_fn(1, 'a')"));
            assertEquals("updated", query1(s, "SELECT ups_fn(1, 'b')"));
            assertEquals("b", query1(s, "SELECT v FROM ups_t WHERE k = 1"));
            assertEquals("1", query1(s, "SELECT count(*) FROM ups_t"));

            s.execute("DROP FUNCTION ups_fn(int, text)");
            s.execute("DROP TABLE ups_t CASCADE");
        }
    }

    // =========================================================================
    // 4. Variable vs column name conflicts
    // =========================================================================

    @Test
    void ambiguous_variable_column_raises_42702() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE amb_emp (salary int)");
            s.execute("INSERT INTO amb_emp VALUES (100), (200)");
            s.execute("""
                CREATE OR REPLACE FUNCTION amb_sum() RETURNS bigint LANGUAGE plpgsql AS $$
                DECLARE salary int := 999;
                BEGIN
                  RETURN (SELECT sum(salary) FROM amb_emp);
                END;
                $$""");

            SQLException ex = assertThrows(SQLException.class, () -> {
                try (ResultSet rs = s.executeQuery("SELECT amb_sum()")) {
                    rs.next();
                }
            });
            assertEquals("42702", ex.getSQLState());
            assertTrue(ex.getMessage().contains("ambiguous"),
                    "message should mention ambiguity: " + ex.getMessage());

            s.execute("DROP FUNCTION amb_sum()");
            s.execute("DROP TABLE amb_emp");
        }
    }

    @Test
    void update_set_target_is_not_substituted() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ut_t (id int PRIMARY KEY, counter int)");
            s.execute("INSERT INTO ut_t VALUES (1, 0)");
            s.execute("""
                CREATE OR REPLACE FUNCTION ut_fn() RETURNS int LANGUAGE plpgsql AS $$
                DECLARE counter int := 99;
                BEGIN
                  UPDATE ut_t SET counter = 5 WHERE id = 1;
                  RETURN counter;  -- the variable, still 99
                END;
                $$""");

            assertEquals("99", query1(s, "SELECT ut_fn()"));
            assertEquals("5", query1(s, "SELECT counter FROM ut_t WHERE id = 1"));

            s.execute("DROP FUNCTION ut_fn()");
            s.execute("DROP TABLE ut_t");
        }
    }

    @Test
    void qualified_alias_column_still_resolves_as_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE qa_emp (salary int)");
            s.execute("INSERT INTO qa_emp VALUES (100), (200)");
            s.execute("""
                CREATE OR REPLACE FUNCTION qa_fn() RETURNS bigint LANGUAGE plpgsql AS $$
                DECLARE salary int := 999; total bigint;
                BEGIN
                  SELECT sum(e.salary) INTO total FROM qa_emp e;
                  RETURN total;
                END;
                $$""");

            assertEquals("300", query1(s, "SELECT qa_fn()"));

            s.execute("DROP FUNCTION qa_fn()");
            s.execute("DROP TABLE qa_emp");
        }
    }

    @Test
    void function_name_qualified_parameter_resolves_as_variable() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bq_t (cnt int)");
            s.execute("INSERT INTO bq_t VALUES (5)");
            // The documented PG idiom: funcname.param disambiguates a parameter from a column
            s.execute("""
                CREATE OR REPLACE FUNCTION bq_fn(cnt int) RETURNS bigint LANGUAGE plpgsql AS $$
                DECLARE total bigint;
                BEGIN
                  SELECT sum(t.cnt) + bq_fn.cnt INTO total FROM bq_t t;
                  RETURN total;
                END;
                $$""");

            assertEquals("12", query1(s, "SELECT bq_fn(7)"));

            s.execute("DROP FUNCTION bq_fn(int)");
            s.execute("DROP TABLE bq_t");
        }
    }

    @Test
    void function_name_qualified_declared_variable_raises_42p01() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bqd_t (cnt int)");
            s.execute("INSERT INTO bqd_t VALUES (5)");
            // In PG, the block labeled with the function name contains ONLY the parameters.
            // funcname.declared_var falls through to table resolution and fails with 42P01.
            s.execute("""
                CREATE OR REPLACE FUNCTION bqd_fn() RETURNS bigint LANGUAGE plpgsql AS $$
                DECLARE cnt int := 7; total bigint;
                BEGIN
                  SELECT sum(t.cnt) + bqd_fn.cnt INTO total FROM bqd_t t;
                  RETURN total;
                END;
                $$""");

            SQLException ex = assertThrows(SQLException.class, () -> {
                try (ResultSet rs = s.executeQuery("SELECT bqd_fn()")) {
                    rs.next();
                }
            });
            assertEquals("42P01", ex.getSQLState());
            assertTrue(ex.getMessage().contains("missing FROM-clause entry for table \"bqd_fn\""),
                    "unexpected message: " + ex.getMessage());

            s.execute("DROP FUNCTION bqd_fn()");
            s.execute("DROP TABLE bqd_t");
        }
    }

    // =========================================================================
    // 5. EXECUTE ... USING parameter splicing
    // =========================================================================

    @Test
    void for_execute_using_string_param_matches_rows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dyn_t (id int PRIMARY KEY, name text)");
            s.execute("INSERT INTO dyn_t VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')");
            s.execute("""
                CREATE OR REPLACE FUNCTION dyn_count(p_name text) RETURNS int LANGUAGE plpgsql AS $$
                DECLARE c int := 0; r record;
                BEGIN
                  FOR r IN EXECUTE 'SELECT id FROM dyn_t WHERE name = $1' USING p_name LOOP
                    c := c + 1;
                  END LOOP;
                  RETURN c;
                END;
                $$""");

            assertEquals("2", query1(s, "SELECT dyn_count('alice')"));
            assertEquals("1", query1(s, "SELECT dyn_count('bob')"));
            assertEquals("0", query1(s, "SELECT dyn_count('nobody')"));

            s.execute("DROP FUNCTION dyn_count(text)");
            s.execute("DROP TABLE dyn_t");
        }
    }

    @Test
    void for_execute_using_with_more_than_nine_params() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                CREATE OR REPLACE FUNCTION dyn_many() RETURNS text LANGUAGE plpgsql AS $$
                DECLARE r record; res text;
                BEGIN
                  FOR r IN EXECUTE 'SELECT $1 || $10 || $11 AS x'
                      USING 'a','b','c','d','e','f','g','h','i','j','k' LOOP
                    res := r.x;
                  END LOOP;
                  RETURN res;
                END;
                $$""");

            assertEquals("ajk", query1(s, "SELECT dyn_many()"),
                    "$10/$11 must not be corrupted by the $1 substitution");

            s.execute("DROP FUNCTION dyn_many()");
        }
    }

    @Test
    void for_execute_using_quotes_special_characters() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dynq_t (name text)");
            s.execute("INSERT INTO dynq_t VALUES ('o''brien')");
            s.execute("""
                CREATE OR REPLACE FUNCTION dynq_fn(p text) RETURNS int LANGUAGE plpgsql AS $$
                DECLARE c int := 0; r record;
                BEGIN
                  FOR r IN EXECUTE 'SELECT 1 AS one FROM dynq_t WHERE name = $1' USING p LOOP
                    c := c + 1;
                  END LOOP;
                  RETURN c;
                END;
                $$""");

            assertEquals("1", query1(s, "SELECT dynq_fn('o''brien')"));

            s.execute("DROP FUNCTION dynq_fn(text)");
            s.execute("DROP TABLE dynq_t");
        }
    }

    // =========================================================================
    // 6. SELECT INTO / EXECUTE INTO with zero rows
    // =========================================================================

    @Test
    void select_into_zero_rows_sets_targets_to_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zi_t (id int PRIMARY KEY, note text)");
            s.execute("""
                CREATE OR REPLACE FUNCTION zi_fn() RETURNS text LANGUAGE plpgsql AS $$
                DECLARE v text := 'sentinel';
                BEGIN
                  SELECT note INTO v FROM zi_t WHERE id = -1;
                  RETURN coalesce(v, 'was-null');
                END;
                $$""");

            assertEquals("was-null", query1(s, "SELECT zi_fn()"));

            s.execute("DROP FUNCTION zi_fn()");
            s.execute("DROP TABLE zi_t");
        }
    }

    @Test
    void select_into_zero_rows_nulls_all_targets() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zi2_t (a int, b int)");
            s.execute("""
                CREATE OR REPLACE FUNCTION zi2_fn() RETURNS text LANGUAGE plpgsql AS $$
                DECLARE x int := 1; y int := 2;
                BEGIN
                  SELECT a, b INTO x, y FROM zi2_t WHERE a = -1;
                  RETURN coalesce(x::text, 'x-null') || ',' || coalesce(y::text, 'y-null');
                END;
                $$""");

            assertEquals("x-null,y-null", query1(s, "SELECT zi2_fn()"));

            s.execute("DROP FUNCTION zi2_fn()");
            s.execute("DROP TABLE zi2_t");
        }
    }

    @Test
    void execute_into_zero_rows_sets_targets_to_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                CREATE OR REPLACE FUNCTION ze_fn() RETURNS text LANGUAGE plpgsql AS $$
                DECLARE v text := 'sentinel';
                BEGIN
                  EXECUTE 'SELECT ''x'' WHERE 1 = 0' INTO v;
                  RETURN coalesce(v, 'was-null');
                END;
                $$""");

            assertEquals("was-null", query1(s, "SELECT ze_fn()"));

            s.execute("DROP FUNCTION ze_fn()");
        }
    }

    // =========================================================================
    // 7. FOR loop variable scoping
    // =========================================================================

    @Test
    void integer_for_loop_variable_does_not_clobber_outer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                CREATE OR REPLACE FUNCTION lv_fn() RETURNS int LANGUAGE plpgsql AS $$
                DECLARE i int := 100; s int := 0;
                BEGIN
                  FOR i IN 1..3 LOOP s := s + i; END LOOP;
                  RETURN i * 1000 + s;  -- outer i must still be 100
                END;
                $$""");

            assertEquals("100006", query1(s, "SELECT lv_fn()"));

            s.execute("DROP FUNCTION lv_fn()");
        }
    }

    /**
     * The target of a FOR over rows is the variable the block declared, not a fresh one: PG 18
     * writes each row into it and leaves the last one there after the loop. (Only the integer
     * FOR defines a variable of its own, and only an inner block's own DECLARE shadows.) The
     * live server returns 2:2 for this function, so 'outer' was never what it kept.
     */
    @Test
    void record_for_loop_variable_is_the_declared_one() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lvr_t (id int)");
            s.execute("INSERT INTO lvr_t VALUES (1), (2)");
            s.execute("""
                CREATE OR REPLACE FUNCTION lvr_fn() RETURNS text LANGUAGE plpgsql AS $$
                DECLARE r text := 'outer'; n int := 0;
                BEGIN
                  FOR r IN SELECT id FROM lvr_t ORDER BY id LOOP n := n + 1; END LOOP;
                  RETURN r || ':' || n;
                END;
                $$""");

            assertEquals("2:2", query1(s, "SELECT lvr_fn()"));

            // An inner block that declares its own r is the one thing that does shadow
            s.execute("""
                CREATE OR REPLACE FUNCTION lvr_fn2() RETURNS text LANGUAGE plpgsql AS $$
                DECLARE r text := 'outer';
                BEGIN
                  DECLARE r record;
                  BEGIN
                    FOR r IN SELECT id FROM lvr_t LOOP NULL; END LOOP;
                  END;
                  RETURN r;
                END;
                $$""");
            assertEquals("outer", query1(s, "SELECT lvr_fn2()"));

            s.execute("DROP FUNCTION lvr_fn()");
            s.execute("DROP FUNCTION lvr_fn2()");
            s.execute("DROP TABLE lvr_t");
        }
    }

    // =========================================================================
    // 8. FOR ... BY 0
    // =========================================================================

    @Test
    void for_loop_by_zero_raises_22023() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(20); // safety net: without the fix this loops forever
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("DO $$ BEGIN FOR i IN 1..3 BY 0 LOOP NULL; END LOOP; END $$"));
            assertEquals("22023", ex.getSQLState());
            assertTrue(ex.getMessage().contains("BY value of FOR loop must be greater than zero"),
                    "unexpected message: " + ex.getMessage());
        }
    }

    @Test
    void reverse_for_loop_by_zero_raises_22023() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(20);
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("DO $$ BEGIN FOR i IN REVERSE 3..1 BY 0 LOOP NULL; END LOOP; END $$"));
            assertEquals("22023", ex.getSQLState());
        }
    }

    /**
     * A width is read where the declaration is read: a variable no value could ever fill is
     * refused as the declaration is made, exactly as a column of that width is.
     */
    @Test
    void a_declared_width_no_value_could_fill() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> s.execute(
                    "CREATE FUNCTION plf_charbig() RETURNS int AS $$"
                            + " DECLARE v char(20000000) := 'a'; BEGIN RETURN length(v); END $$"
                            + " LANGUAGE plpgsql"));
            assertEquals("22023", ex.getSQLState());
            assertTrue(ex.getMessage().contains("length for type char cannot exceed 10485760"),
                    "unexpected message: " + ex.getMessage());
            // A width a value can fill is declared without complaint.
            s.execute("CREATE FUNCTION plf_charok() RETURNS int AS $$"
                    + " DECLARE v char(20) := 'a'; BEGIN RETURN length(v); END $$ LANGUAGE plpgsql");
            s.execute("DROP FUNCTION plf_charok()");
        }
    }

    /**
     * A record has the fields of whatever row was put in it, so until one has been there is no
     * field to read: PostgreSQL says which variable was never assigned, and a row put in by
     * ROW(...) carries values and no names, so no name reaches a field of that either.
     */
    @Test
    void a_record_read_before_a_row_was_put_in_it() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException never = assertThrows(SQLException.class, () -> s.execute(
                    "DO $$ DECLARE r record; BEGIN IF r.nm IS NULL THEN NULL; END IF; END $$"));
            assertEquals("55000", never.getSQLState());
            assertTrue(never.getMessage().contains("record \"r\" is not assigned yet"),
                    "unexpected message: " + never.getMessage());
            SQLException nameless = assertThrows(SQLException.class, () -> s.execute(
                    "DO $$ DECLARE r record; BEGIN r := row(1);"
                            + " IF r.nm IS NULL THEN NULL; END IF; END $$"));
            assertEquals("42703", nameless.getSQLState());
            assertTrue(nameless.getMessage().contains("record \"r\" has no field \"nm\""),
                    "unexpected message: " + nameless.getMessage());
        }
    }

    /**
     * A statement that was cancelled is not caught by WHEN OTHERS: the client asked for it to
     * stop, and a block that swallowed it would go on running.
     */
    @Test
    void what_others_does_not_catch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException cancelled = assertThrows(SQLException.class, () -> s.execute(
                    "DO $$ BEGIN RAISE SQLSTATE '57014'; EXCEPTION WHEN OTHERS THEN NULL; END $$"));
            assertEquals("57014", cancelled.getSQLState());
            // Caught by its own name, it is caught.
            s.execute("DO $$ BEGIN RAISE SQLSTATE '57014';"
                    + " EXCEPTION WHEN query_canceled THEN NULL; END $$");
            // Every other error is what OTHERS is for.
            s.execute("DO $$ BEGIN RAISE SQLSTATE '22012';"
                    + " EXCEPTION WHEN OTHERS THEN NULL; END $$");
        }
    }

    /**
     * A collation says which order text is compared in, so it may only be declared on a variable
     * whose values compare as text: a number is compared by what it is, and a collation written
     * on one is refused rather than quietly ignored.
     */
    @Test
    void a_collation_declared_on_a_type_that_has_none() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("DO $$ DECLARE x int COLLATE \"C\"; BEGIN NULL; END $$"));
            assertEquals("42804", ex.getSQLState());
            assertTrue(ex.getMessage().contains("collations are not supported by type integer"),
                    "unexpected message: " + ex.getMessage());
            assertEquals("42804", assertThrows(SQLException.class, () ->
                    s.execute("DO $$ DECLARE x date COLLATE \"C\"; BEGIN NULL; END $$"))
                    .getSQLState());
            // The types that do compare as text take one.
            s.execute("DO $$ DECLARE x text COLLATE \"C\"; BEGIN NULL; END $$");
            s.execute("DO $$ DECLARE x varchar(5) COLLATE \"C\"; BEGIN NULL; END $$");
            s.execute("DO $$ DECLARE x text[] COLLATE \"C\"; BEGIN NULL; END $$");
        }
    }
}
