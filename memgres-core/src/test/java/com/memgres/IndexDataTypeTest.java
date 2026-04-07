package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that indexes work correctly across all data types,
 * composite keys with mixed types, partial indexes, and cross-type scenarios.
 */
class IndexDataTypeTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ======================== Scalar PK types ========================

    @Test
    void uuidPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_uuid (id UUID PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'one')");
            s.execute("INSERT INTO idx_uuid VALUES ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', 'two')");
            // Duplicate UUID should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'dup')"));
            // Delete and reinsert should work
            s.execute("DELETE FROM idx_uuid WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
            s.execute("INSERT INTO idx_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'reused')");
            ResultSet rs = s.executeQuery("SELECT val FROM idx_uuid WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
            assertTrue(rs.next());
            assertEquals("reused", rs.getString(1));
        }
    }

    @Test
    void booleanUnique() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_bool (flag BOOLEAN UNIQUE, label TEXT)");
            s.execute("INSERT INTO idx_bool VALUES (TRUE, 'yes')");
            s.execute("INSERT INTO idx_bool VALUES (FALSE, 'no')");
            // Only two possible values, so a third should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_bool VALUES (TRUE, 'again')"));
            // NULL should be allowed multiple times
            s.execute("INSERT INTO idx_bool VALUES (NULL, 'n1')");
            s.execute("INSERT INTO idx_bool VALUES (NULL, 'n2')");
        }
    }

    @Test
    void datePrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_date (d DATE PRIMARY KEY, event TEXT)");
            s.execute("INSERT INTO idx_date VALUES ('2024-01-01', 'new year')");
            s.execute("INSERT INTO idx_date VALUES ('2024-12-25', 'christmas')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_date VALUES ('2024-01-01', 'dup')"));
            // Update PK and reuse old value
            s.execute("UPDATE idx_date SET d = '2024-01-02' WHERE d = '2024-01-01'");
            s.execute("INSERT INTO idx_date VALUES ('2024-01-01', 'reinserted')");
        }
    }

    @Test
    void timestampPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_ts (ts TIMESTAMP PRIMARY KEY, note TEXT)");
            s.execute("INSERT INTO idx_ts VALUES ('2024-01-01 12:00:00', 'noon')");
            s.execute("INSERT INTO idx_ts VALUES ('2024-01-01 13:00:00', '1pm')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_ts VALUES ('2024-01-01 12:00:00', 'dup')"));
        }
    }

    @Test
    void timestamptzPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_tstz (ts TIMESTAMPTZ PRIMARY KEY, note TEXT)");
            s.execute("INSERT INTO idx_tstz VALUES ('2024-01-01 12:00:00+00', 'utc noon')");
            s.execute("INSERT INTO idx_tstz VALUES ('2024-01-01 13:00:00+00', 'utc 1pm')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_tstz VALUES ('2024-01-01 12:00:00+00', 'dup')"));
        }
    }

    @Test
    void timePrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_time (t TIME PRIMARY KEY, label TEXT)");
            s.execute("INSERT INTO idx_time VALUES ('08:00:00', 'morning')");
            s.execute("INSERT INTO idx_time VALUES ('17:00:00', 'evening')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_time VALUES ('08:00:00', 'dup')"));
        }
    }

    @Test
    void intervalUnique() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_interval (dur INTERVAL UNIQUE, label TEXT)");
            s.execute("INSERT INTO idx_interval VALUES ('1 hour', 'short')");
            s.execute("INSERT INTO idx_interval VALUES ('2 hours', 'medium')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_interval VALUES ('1 hour', 'dup')"));
        }
    }

    @Test
    void varcharPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_vc (code VARCHAR(10) PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_vc VALUES ('ABC', 'first')");
            s.execute("INSERT INTO idx_vc VALUES ('DEF', 'second')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_vc VALUES ('ABC', 'dup')"));
            // Case-sensitive
            s.execute("INSERT INTO idx_vc VALUES ('abc', 'lowercase ok')");
        }
    }

    @Test
    void textPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_text (name TEXT PRIMARY KEY)");
            s.execute("INSERT INTO idx_text VALUES ('Alice')");
            s.execute("INSERT INTO idx_text VALUES ('Bob')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_text VALUES ('Alice')"));
        }
    }

    @Test
    void numericPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_numeric (n NUMERIC(10,2) PRIMARY KEY, label TEXT)");
            s.execute("INSERT INTO idx_numeric VALUES (1.50, 'one-fifty')");
            s.execute("INSERT INTO idx_numeric VALUES (2.75, 'two-seventy-five')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_numeric VALUES (1.50, 'dup')"));
            // 1.5 and 1.50 should be the same after normalization
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_numeric VALUES (1.5, 'dup-trailing')"));
        }
    }

    @Test
    void realPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_real (r REAL PRIMARY KEY)");
            s.execute("INSERT INTO idx_real VALUES (1.5)");
            s.execute("INSERT INTO idx_real VALUES (2.5)");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_real VALUES (1.5)"));
        }
    }

    @Test
    void doublePrecisionPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_dp (d DOUBLE PRECISION PRIMARY KEY)");
            s.execute("INSERT INTO idx_dp VALUES (3.14159)");
            s.execute("INSERT INTO idx_dp VALUES (2.71828)");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_dp VALUES (3.14159)"));
        }
    }

    @Test
    void smallintPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_si (id SMALLINT PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_si VALUES (1, 'a')");
            s.execute("INSERT INTO idx_si VALUES (2, 'b')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_si VALUES (1, 'dup')"));
        }
    }

    @Test
    void bigintPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_bi (id BIGINT PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_bi VALUES (9999999999, 'big')");
            s.execute("INSERT INTO idx_bi VALUES (8888888888, 'also big')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_bi VALUES (9999999999, 'dup')"));
        }
    }

    @Test
    void byteaPrimaryKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_bytea (data BYTEA PRIMARY KEY, label TEXT)");
            s.execute("INSERT INTO idx_bytea VALUES (E'\\\\x0102', 'first')");
            s.execute("INSERT INTO idx_bytea VALUES (E'\\\\x0304', 'second')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_bytea VALUES (E'\\\\x0102', 'dup')"));
        }
    }

    @Test
    void jsonbUnique() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_jsonb (data JSONB UNIQUE, label TEXT)");
            s.execute("INSERT INTO idx_jsonb VALUES ('{\"a\":1}', 'first')");
            s.execute("INSERT INTO idx_jsonb VALUES ('{\"b\":2}', 'second')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_jsonb VALUES ('{\"a\":1}', 'dup')"));
            // Key order shouldn't matter for JSONB
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_jsonb VALUES ('{\"a\": 1}', 'dup-reformat')"));
        }
    }

    @Test
    void enumUnique() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE idx_color AS ENUM ('red', 'green', 'blue')");
            s.execute("CREATE TABLE idx_enum (c idx_color UNIQUE, label TEXT)");
            s.execute("INSERT INTO idx_enum VALUES ('red', 'r')");
            s.execute("INSERT INTO idx_enum VALUES ('green', 'g')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_enum VALUES ('red', 'dup')"));
        }
    }

    @Test
    void inetUnique() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_inet (addr INET UNIQUE)");
            s.execute("INSERT INTO idx_inet VALUES ('192.168.1.1')");
            s.execute("INSERT INTO idx_inet VALUES ('10.0.0.1')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_inet VALUES ('192.168.1.1')"));
        }
    }

    // ======================== Cross-type numeric index ========================

    @Test
    void crossTypeIntegerBigintConflict() throws SQLException {
        // INTEGER PK, insert via BIGINT-typed value; should conflict
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_cross_num (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_cross_num VALUES (42, 'int')");
            // The JDBC driver sends this as Integer, but the normalization should still match
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_cross_num VALUES (42, 'dup')"));
        }
    }

    @Test
    void crossTypeBigintFkToIntegerPk() throws SQLException {
        // Parent with INTEGER PK, child with BIGINT FK
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_xfk_parent (id INTEGER PRIMARY KEY)");
            s.execute("CREATE TABLE idx_xfk_child (id SERIAL PRIMARY KEY, pid BIGINT REFERENCES idx_xfk_parent(id))");
            s.execute("INSERT INTO idx_xfk_parent VALUES (1), (2)");
            s.execute("INSERT INTO idx_xfk_child (pid) VALUES (1)");
            s.execute("INSERT INTO idx_xfk_child (pid) VALUES (2)");
            // FK to nonexistent parent should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_xfk_child (pid) VALUES (999)"));
        }
    }

    @Test
    void numericEquivalenceInIndex() throws SQLException {
        // 1, 1.0, 1.00 should all be the same in index
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_num_equiv (n NUMERIC UNIQUE)");
            s.execute("INSERT INTO idx_num_equiv VALUES (1)");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_num_equiv VALUES (1.0)"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_num_equiv VALUES (1.00)"));
        }
    }

    // ======================== Composite keys with mixed types ========================

    @Test
    void compositeIntegerText() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_comp_it (a INTEGER, b TEXT, PRIMARY KEY (a, b))");
            s.execute("INSERT INTO idx_comp_it VALUES (1, 'x')");
            s.execute("INSERT INTO idx_comp_it VALUES (1, 'y')"); // same a, different b
            s.execute("INSERT INTO idx_comp_it VALUES (2, 'x')"); // different a, same b
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_comp_it VALUES (1, 'x')")); // exact duplicate
        }
    }

    @Test
    void compositeUuidDate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_comp_ud (u UUID, d DATE, UNIQUE(u, d))");
            s.execute("INSERT INTO idx_comp_ud VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01')");
            s.execute("INSERT INTO idx_comp_ud VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-02')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_comp_ud VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01')"));
        }
    }

    @Test
    void compositeThreeColumns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_comp3 (a INTEGER, b TEXT, c BOOLEAN, UNIQUE(a, b, c))");
            s.execute("INSERT INTO idx_comp3 VALUES (1, 'x', TRUE)");
            s.execute("INSERT INTO idx_comp3 VALUES (1, 'x', FALSE)"); // differs in c
            s.execute("INSERT INTO idx_comp3 VALUES (1, 'y', TRUE)"); // differs in b
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_comp3 VALUES (1, 'x', TRUE)")); // exact dup
        }
    }

    @Test
    void compositeIntegerTimestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_comp_its (tenant_id INTEGER, created_at TIMESTAMP, data TEXT, PRIMARY KEY (tenant_id, created_at))");
            s.execute("INSERT INTO idx_comp_its VALUES (1, '2024-01-01 00:00:00', 'a')");
            s.execute("INSERT INTO idx_comp_its VALUES (1, '2024-01-02 00:00:00', 'b')");
            s.execute("INSERT INTO idx_comp_its VALUES (2, '2024-01-01 00:00:00', 'c')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_comp_its VALUES (1, '2024-01-01 00:00:00', 'dup')"));
        }
    }

    @Test
    void compositeWithNullInOneColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_comp_null2 (a INTEGER, b TEXT, UNIQUE(a, b))");
            // (1, NULL): NULLs are distinct, so multiple allowed
            s.execute("INSERT INTO idx_comp_null2 VALUES (1, NULL)");
            s.execute("INSERT INTO idx_comp_null2 VALUES (1, NULL)");
            // (NULL, 'x'): also distinct
            s.execute("INSERT INTO idx_comp_null2 VALUES (NULL, 'x')");
            s.execute("INSERT INTO idx_comp_null2 VALUES (NULL, 'x')");
            // Non-null duplicates should fail
            s.execute("INSERT INTO idx_comp_null2 VALUES (1, 'a')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_comp_null2 VALUES (1, 'a')"));
        }
    }

    // ======================== Composite ON CONFLICT ========================

    @Test
    void onConflictCompositeIntText() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_oc_comp_it (a INTEGER, b TEXT, val TEXT, PRIMARY KEY (a, b))");
            s.execute("INSERT INTO idx_oc_comp_it VALUES (1, 'x', 'orig')");
            s.execute("INSERT INTO idx_oc_comp_it VALUES (1, 'x', 'updated') ON CONFLICT (a, b) DO UPDATE SET val = EXCLUDED.val");
            ResultSet rs = s.executeQuery("SELECT val FROM idx_oc_comp_it WHERE a = 1 AND b = 'x'");
            assertTrue(rs.next());
            assertEquals("updated", rs.getString(1));
            rs = s.executeQuery("SELECT count(*) FROM idx_oc_comp_it");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void onConflictUuidPk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_oc_uuid (id UUID PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_oc_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'orig')");
            s.execute("INSERT INTO idx_oc_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'updated') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
            ResultSet rs = s.executeQuery("SELECT val FROM idx_oc_uuid");
            assertTrue(rs.next());
            assertEquals("updated", rs.getString(1));
        }
    }

    // ======================== Partial (WHERE) unique indexes ========================

    @Test
    void partialUniqueIndexBasic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Only active users must have unique emails
            s.execute("CREATE TABLE idx_partial (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)");
            s.execute("CREATE UNIQUE INDEX idx_partial_email ON idx_partial (email) WHERE active = TRUE");
            s.execute("INSERT INTO idx_partial (email, active) VALUES ('alice@test.com', TRUE)");
            s.execute("INSERT INTO idx_partial (email, active) VALUES ('bob@test.com', TRUE)");
            // Duplicate email with active=TRUE should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_partial (email, active) VALUES ('alice@test.com', TRUE)"));
            // Same email with active=FALSE should be allowed (not covered by partial index)
            s.execute("INSERT INTO idx_partial (email, active) VALUES ('alice@test.com', FALSE)");
            // Another inactive duplicate also fine
            s.execute("INSERT INTO idx_partial (email, active) VALUES ('alice@test.com', FALSE)");
        }
    }

    @Test
    void partialUniqueIndexWithNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_partial_null (id SERIAL PRIMARY KEY, code TEXT, deleted_at TIMESTAMP)");
            s.execute("CREATE UNIQUE INDEX idx_partial_code ON idx_partial_null (code) WHERE deleted_at IS NULL");
            s.execute("INSERT INTO idx_partial_null (code) VALUES ('A')");
            s.execute("INSERT INTO idx_partial_null (code) VALUES ('B')");
            // Duplicate with NULL deleted_at should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_partial_null (code) VALUES ('A')"));
            // Soft-deleted row should allow reuse
            s.execute("UPDATE idx_partial_null SET deleted_at = NOW() WHERE code = 'A'");
            s.execute("INSERT INTO idx_partial_null (code) VALUES ('A')"); // new active 'A'
        }
    }

    // ======================== FK with different types referencing indexed PK ========================

    @Test
    void fkCascadeWithUuidPk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_fk_uuid_p (id UUID PRIMARY KEY, name TEXT)");
            s.execute("CREATE TABLE idx_fk_uuid_c (id SERIAL PRIMARY KEY, pid UUID REFERENCES idx_fk_uuid_p(id) ON DELETE CASCADE)");
            s.execute("INSERT INTO idx_fk_uuid_p VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'parent')");
            s.execute("INSERT INTO idx_fk_uuid_c (pid) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            s.execute("DELETE FROM idx_fk_uuid_p WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_fk_uuid_c");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void fkCascadeWithTextPk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_fk_text_p (code TEXT PRIMARY KEY)");
            s.execute("CREATE TABLE idx_fk_text_c (id SERIAL PRIMARY KEY, pcode TEXT REFERENCES idx_fk_text_p(code) ON DELETE CASCADE)");
            s.execute("INSERT INTO idx_fk_text_p VALUES ('AAA'), ('BBB')");
            s.execute("INSERT INTO idx_fk_text_c (pcode) VALUES ('AAA'), ('AAA'), ('BBB')");
            s.execute("DELETE FROM idx_fk_text_p WHERE code = 'AAA'");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_fk_text_c");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void fkCascadeUpdateWithDatePk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_fk_date_p (d DATE PRIMARY KEY)");
            s.execute("CREATE TABLE idx_fk_date_c (id SERIAL PRIMARY KEY, pd DATE REFERENCES idx_fk_date_p(d) ON UPDATE CASCADE)");
            s.execute("INSERT INTO idx_fk_date_p VALUES ('2024-01-01')");
            s.execute("INSERT INTO idx_fk_date_c (pd) VALUES ('2024-01-01')");
            s.execute("UPDATE idx_fk_date_p SET d = '2025-01-01' WHERE d = '2024-01-01'");
            ResultSet rs = s.executeQuery("SELECT pd FROM idx_fk_date_c");
            assertTrue(rs.next());
            assertEquals(java.sql.Date.valueOf("2025-01-01"), rs.getDate(1));
        }
    }

    // ======================== Multiple unique constraints on different types ========================

    @Test
    void multipleUniqueConstraintsDifferentTypes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_multi_types (id SERIAL PRIMARY KEY, email TEXT UNIQUE, badge_num INTEGER UNIQUE, hire_date DATE UNIQUE)");
            s.execute("INSERT INTO idx_multi_types (email, badge_num, hire_date) VALUES ('a@t.com', 100, '2024-01-01')");
            s.execute("INSERT INTO idx_multi_types (email, badge_num, hire_date) VALUES ('b@t.com', 200, '2024-02-01')");
            // Each constraint checked independently
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_multi_types (email, badge_num, hire_date) VALUES ('a@t.com', 300, '2024-03-01')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_multi_types (email, badge_num, hire_date) VALUES ('c@t.com', 100, '2024-03-01')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_multi_types (email, badge_num, hire_date) VALUES ('c@t.com', 300, '2024-01-01')"));
        }
    }

    // ======================== Rollback consistency across types ========================

    @Test
    void rollbackWithUuidPk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_rb_uuid (id UUID PRIMARY KEY, val TEXT)");
            conn.setAutoCommit(false);
            s.execute("INSERT INTO idx_rb_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'will rollback')");
            conn.rollback();
            conn.setAutoCommit(true);
            // Should be free after rollback
            s.execute("INSERT INTO idx_rb_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'after rollback')");
            ResultSet rs = s.executeQuery("SELECT val FROM idx_rb_uuid");
            assertTrue(rs.next());
            assertEquals("after rollback", rs.getString(1));
        }
    }

    @Test
    void rollbackWithDatePk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_rb_date (d DATE PRIMARY KEY)");
            conn.setAutoCommit(false);
            s.execute("INSERT INTO idx_rb_date VALUES ('2024-06-15')");
            conn.rollback();
            conn.setAutoCommit(true);
            s.execute("INSERT INTO idx_rb_date VALUES ('2024-06-15')");
        }
    }

    @Test
    void savepointRollbackWithTextPk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_sp_text (code TEXT PRIMARY KEY, val TEXT)");
            conn.setAutoCommit(false);
            s.execute("INSERT INTO idx_sp_text VALUES ('AAA', 'committed')");
            Savepoint sp = conn.setSavepoint("sp1");
            s.execute("INSERT INTO idx_sp_text VALUES ('BBB', 'will rollback')");
            conn.rollback(sp);
            // BBB should be free after savepoint rollback
            s.execute("INSERT INTO idx_sp_text VALUES ('BBB', 'after sp rollback')");
            conn.commit();
            conn.setAutoCommit(true);
            ResultSet rs = s.executeQuery("SELECT val FROM idx_sp_text WHERE code = 'BBB'");
            assertTrue(rs.next());
            assertEquals("after sp rollback", rs.getString(1));
        }
    }

    // ======================== Update PK to different type-equivalent value ========================

    @Test
    void updateNumericPkEquivalent() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_upd_num (n NUMERIC PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_upd_num VALUES (1.0, 'one')");
            s.execute("INSERT INTO idx_upd_num VALUES (2.0, 'two')");
            // Update 1.0 to 1.00, same value so it should work (self-update)
            s.execute("UPDATE idx_upd_num SET n = 1.00 WHERE n = 1.0");
            // Verify it's still there
            ResultSet rs = s.executeQuery("SELECT val FROM idx_upd_num WHERE n = 1");
            assertTrue(rs.next());
            assertEquals("one", rs.getString(1));
        }
    }

    // ======================== Bulk operations across types ========================

    @Test
    void bulkInsertWithDatePk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_bulk_date (d DATE PRIMARY KEY, val TEXT)");
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO idx_bulk_date VALUES (?, ?)")) {
            for (int i = 0; i < 365; i++) {
                ps.setDate(1, java.sql.Date.valueOf(java.time.LocalDate.of(2024, 1, 1).plusDays(i)));
                ps.setString(2, "day" + i);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        // All 365 dates should be in
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_bulk_date");
            assertTrue(rs.next());
            assertEquals(365, rs.getInt(1));
            // Duplicate should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_bulk_date VALUES ('2024-01-01', 'dup')"));
        }
    }

    @Test
    void bulkInsertWithUuidPk() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_bulk_uuid (id UUID PRIMARY KEY, val TEXT)");
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO idx_bulk_uuid VALUES (gen_random_uuid(), ?)")) {
            for (int i = 0; i < 1000; i++) {
                ps.setString(1, "row" + i);
                ps.addBatch();
                if (i % 100 == 99) ps.executeBatch();
            }
            ps.executeBatch();
        }
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_bulk_uuid");
            assertTrue(rs.next());
            assertEquals(1000, rs.getInt(1));
        }
    }

    // ======================== Composite FK with mixed types ========================

    @Test
    void compositeFkMixedTypes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_cfk_p (tenant_id INTEGER, code TEXT, PRIMARY KEY (tenant_id, code))");
            s.execute("CREATE TABLE idx_cfk_c (id SERIAL PRIMARY KEY, tenant_id INTEGER, code TEXT, FOREIGN KEY (tenant_id, code) REFERENCES idx_cfk_p(tenant_id, code) ON DELETE CASCADE)");
            s.execute("INSERT INTO idx_cfk_p VALUES (1, 'A'), (1, 'B'), (2, 'A')");
            s.execute("INSERT INTO idx_cfk_c (tenant_id, code) VALUES (1, 'A'), (1, 'B'), (2, 'A')");
            // FK violation
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_cfk_c (tenant_id, code) VALUES (2, 'B')"));
            // Cascade delete
            s.execute("DELETE FROM idx_cfk_p WHERE tenant_id = 1 AND code = 'A'");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_cfk_c WHERE tenant_id = 1 AND code = 'A'");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }
}
