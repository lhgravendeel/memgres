package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE TABLE syntax variants found in real-world schemas
 * that are not yet supported by memgres.
 */
class CreateTableCompatTest {

    static Memgres memgres;
    static Connection conn;

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

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // timestamp WITH/WITHOUT TIME ZONE: full syntax in column definitions
    // =========================================================================

    @Test
    void testTimestampWithoutTimeZone() throws SQLException {
        exec("CREATE TABLE ts_test1 (id serial PRIMARY KEY, created_at timestamp without time zone NOT NULL)");
        exec("INSERT INTO ts_test1 (created_at) VALUES ('2024-01-15 10:30:00')");
        assertNotNull(query1("SELECT created_at FROM ts_test1"));
    }

    @Test
    void testTimestampWithTimeZone() throws SQLException {
        exec("CREATE TABLE ts_test2 (id serial PRIMARY KEY, created_at timestamp with time zone NOT NULL)");
        exec("INSERT INTO ts_test2 (created_at) VALUES ('2024-01-15 10:30:00+00')");
        assertNotNull(query1("SELECT created_at FROM ts_test2"));
    }

    @Test
    void testTimestamp6WithoutTimeZone() throws SQLException {
        // pg_dump uses timestamp(6) without time zone
        exec("CREATE TABLE ts_test3 (id serial PRIMARY KEY, created_at timestamp(6) without time zone NOT NULL, updated_at timestamp(6) without time zone NOT NULL)");
        exec("INSERT INTO ts_test3 (created_at, updated_at) VALUES ('2024-01-15 10:30:00', '2024-01-15 10:31:00')");
        assertNotNull(query1("SELECT created_at FROM ts_test3"));
    }

    @Test
    void testTimestamp6WithTimeZone() throws SQLException {
        exec("CREATE TABLE ts_test4 (id serial PRIMARY KEY, event_time timestamp(6) with time zone DEFAULT now())");
        exec("INSERT INTO ts_test4 DEFAULT VALUES");
        assertNotNull(query1("SELECT event_time FROM ts_test4"));
    }

    @Test
    void testTimestamp0WithoutTimeZone() throws SQLException {
        // Precision 0 = seconds
        exec("CREATE TABLE ts_test5 (id serial PRIMARY KEY, created_at timestamp(0) without time zone)");
        exec("INSERT INTO ts_test5 (created_at) VALUES ('2024-01-15 10:30:00')");
        assertNotNull(query1("SELECT created_at FROM ts_test5"));
    }

    @Test
    void testTimestamp3() throws SQLException {
        // Some ORMs use TIMESTAMP(3) for millisecond precision
        exec("CREATE TABLE ts_test6 (id serial PRIMARY KEY, created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP)");
        exec("INSERT INTO ts_test6 DEFAULT VALUES");
        assertNotNull(query1("SELECT created_at FROM ts_test6"));
    }

    @Test
    void testTimeWithoutTimeZone() throws SQLException {
        exec("CREATE TABLE time_test1 (id serial PRIMARY KEY, start_time time without time zone)");
        exec("INSERT INTO time_test1 (start_time) VALUES ('10:30:00')");
        assertEquals("10:30:00", query1("SELECT start_time FROM time_test1"));
    }

    @Test
    void testTimeWithTimeZone() throws SQLException {
        exec("CREATE TABLE time_test2 (id serial PRIMARY KEY, start_time time with time zone)");
        exec("INSERT INTO time_test2 (start_time) VALUES ('10:30:00+02')");
        assertNotNull(query1("SELECT start_time FROM time_test2"));
    }

    // =========================================================================
    // character varying (full form used in pg_dump)
    // =========================================================================

    @Test
    void testCharacterVaryingFullForm() throws SQLException {
        // pg_dump outputs "character varying" not "varchar"
        exec("CREATE TABLE cv_test1 (id serial PRIMARY KEY, name character varying NOT NULL, code character varying(10))");
        exec("INSERT INTO cv_test1 (name, code) VALUES ('test', 'ABC')");
        assertEquals("test", query1("SELECT name FROM cv_test1"));
    }

    @Test
    void testCharacterFullForm() throws SQLException {
        exec("CREATE TABLE cv_test2 (id serial PRIMARY KEY, code character(5))");
        exec("INSERT INTO cv_test2 (code) VALUES ('AB')");
        assertNotNull(query1("SELECT code FROM cv_test2"));
    }

    // =========================================================================
    // GENERATED columns: identity and stored
    // =========================================================================

    @Test
    void testGeneratedAlwaysAsIdentity() throws SQLException {
        exec("CREATE TABLE gen_test1 (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text NOT NULL)");
        exec("INSERT INTO gen_test1 (name) VALUES ('Alice')");
        assertEquals("1", query1("SELECT id FROM gen_test1"));
    }

    @Test
    void testGeneratedByDefaultAsIdentity() throws SQLException {
        exec("CREATE TABLE gen_test2 (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name text NOT NULL)");
        exec("INSERT INTO gen_test2 (name) VALUES ('Bob')");
        assertEquals("1", query1("SELECT id FROM gen_test2"));
        // BY DEFAULT allows explicit value
        exec("INSERT INTO gen_test2 (id, name) VALUES (100, 'Charlie')");
        assertEquals("100", query1("SELECT id FROM gen_test2 WHERE name = 'Charlie'"));
    }

    @Test
    void testGeneratedAlwaysAsIdentityWithOptions() throws SQLException {
        exec("CREATE TABLE gen_test3 (id bigint GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10) PRIMARY KEY, name text)");
        exec("INSERT INTO gen_test3 (name) VALUES ('first')");
        assertEquals("100", query1("SELECT id FROM gen_test3"));
        exec("INSERT INTO gen_test3 (name) VALUES ('second')");
        assertEquals("110", query1("SELECT id FROM gen_test3 WHERE name = 'second'"));
    }

    @Test
    void testGeneratedStoredColumn() throws SQLException {
        // GENERATED ALWAYS AS (expr) STORED, i.e. computed columns
        exec("CREATE TABLE gen_test4 (a int, b int, c int GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO gen_test4 (a, b) VALUES (3, 7)");
        assertEquals("10", query1("SELECT c FROM gen_test4"));
    }

    @Test
    void testGeneratedStoredWithTsvector() throws SQLException {
        // Real-world: tsvector search columns
        exec("CREATE TABLE gen_test5 (id serial PRIMARY KEY, name text, search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english', COALESCE(name, ''))) STORED)");
        exec("INSERT INTO gen_test5 (name) VALUES ('Hello World')");
        assertNotNull(query1("SELECT search_vector FROM gen_test5"));
    }

    // =========================================================================
    // PARTITION BY: table partitioning
    // =========================================================================

    @Test
    void testPartitionByRange() throws SQLException {
        exec("CREATE TABLE events (id bigserial, event_date date NOT NULL, payload text) PARTITION BY RANGE (event_date)");
        exec("CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");
        exec("INSERT INTO events (event_date, payload) VALUES ('2024-06-15', 'test')");
        assertEquals("test", query1("SELECT payload FROM events"));
    }

    @Test
    void testPartitionByList() throws SQLException {
        exec("CREATE TABLE orders (id bigserial, region text NOT NULL, amount numeric) PARTITION BY LIST (region)");
        exec("CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('us-east', 'us-west')");
        exec("CREATE TABLE orders_eu PARTITION OF orders FOR VALUES IN ('eu-west', 'eu-central')");
        exec("INSERT INTO orders (region, amount) VALUES ('us-east', 100)");
        assertEquals("100", query1("SELECT amount FROM orders"));
    }

    @Test
    void testPartitionByHash() throws SQLException {
        exec("CREATE TABLE log_entries (id bigserial, data text) PARTITION BY HASH (id)");
        exec("CREATE TABLE log_entries_0 PARTITION OF log_entries FOR VALUES WITH (MODULUS 4, REMAINDER 0)");
        exec("CREATE TABLE log_entries_1 PARTITION OF log_entries FOR VALUES WITH (MODULUS 4, REMAINDER 1)");
        exec("CREATE TABLE log_entries_2 PARTITION OF log_entries FOR VALUES WITH (MODULUS 4, REMAINDER 2)");
        exec("CREATE TABLE log_entries_3 PARTITION OF log_entries FOR VALUES WITH (MODULUS 4, REMAINDER 3)");
        exec("INSERT INTO log_entries (data) VALUES ('test')");
        assertNotNull(query1("SELECT data FROM log_entries"));
    }

    @Test
    void testAttachPartition() throws SQLException {
        exec("CREATE TABLE measurements (id bigserial, ts date NOT NULL, val numeric) PARTITION BY RANGE (ts)");
        exec("CREATE TABLE measurements_q1 (id bigserial, ts date NOT NULL, val numeric)");
        exec("ALTER TABLE measurements ATTACH PARTITION measurements_q1 FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')");
        exec("INSERT INTO measurements (ts, val) VALUES ('2024-02-15', 42)");
        assertEquals("42", query1("SELECT val FROM measurements"));
    }

    @Test
    void testDetachPartition() throws SQLException {
        exec("CREATE TABLE data_parts (id bigserial, period date NOT NULL, info text) PARTITION BY RANGE (period)");
        exec("CREATE TABLE data_parts_jan PARTITION OF data_parts FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')");
        exec("ALTER TABLE data_parts DETACH PARTITION data_parts_jan");
        // After detach, the table still exists standalone
        exec("INSERT INTO data_parts_jan (period, info) VALUES ('2024-01-15', 'standalone')");
        assertEquals("standalone", query1("SELECT info FROM data_parts_jan"));
    }

    // =========================================================================
    // EXCLUDE constraint
    // =========================================================================

    @Test
    void testExcludeConstraintGist() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS btree_gist");
        exec("CREATE TABLE reservations (id serial PRIMARY KEY, room_id int NOT NULL, during daterange NOT NULL, EXCLUDE USING gist (room_id WITH =, during WITH &&))");
        exec("INSERT INTO reservations (room_id, during) VALUES (1, '[2024-01-01, 2024-01-05)')");
        // Overlapping range should be rejected
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO reservations (room_id, during) VALUES (1, '[2024-01-03, 2024-01-07)')"));
    }

    // =========================================================================
    // daterange and other range types
    // =========================================================================

    @Test
    void testDaterangeColumn() throws SQLException {
        exec("CREATE TABLE schedules (id serial PRIMARY KEY, period daterange NOT NULL)");
        exec("INSERT INTO schedules (period) VALUES ('[2024-01-01, 2024-12-31]')");
        assertNotNull(query1("SELECT period FROM schedules"));
    }

    @Test
    void testInt4rangeColumn() throws SQLException {
        exec("CREATE TABLE price_ranges (id serial PRIMARY KEY, amount_range int4range)");
        exec("INSERT INTO price_ranges (amount_range) VALUES ('[10, 100)')");
        assertNotNull(query1("SELECT amount_range FROM price_ranges"));
    }

    @Test
    void testTsrangeColumn() throws SQLException {
        exec("CREATE TABLE bookings (id serial PRIMARY KEY, slot tsrange NOT NULL)");
        exec("INSERT INTO bookings (slot) VALUES ('[2024-01-01 09:00, 2024-01-01 17:00)')");
        assertNotNull(query1("SELECT slot FROM bookings"));
    }

    @Test
    void testTstzrangeColumn() throws SQLException {
        exec("CREATE TABLE shifts (id serial PRIMARY KEY, shift_time tstzrange)");
        exec("INSERT INTO shifts (shift_time) VALUES ('[2024-01-01 09:00+00, 2024-01-01 17:00+00)')");
        assertNotNull(query1("SELECT shift_time FROM shifts"));
    }

    @Test
    void testNumrangeColumn() throws SQLException {
        exec("CREATE TABLE measurements_range (id serial PRIMARY KEY, value_range numrange)");
        exec("INSERT INTO measurements_range (value_range) VALUES ('[1.5, 9.9)')");
        assertNotNull(query1("SELECT value_range FROM measurements_range"));
    }

    // =========================================================================
    // Schema-qualified table names in CREATE TABLE
    // =========================================================================

    @Test
    void testCreateTableInCustomSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS myapp");
        exec("CREATE TABLE myapp.users (id serial PRIMARY KEY, name text NOT NULL)");
        exec("INSERT INTO myapp.users (name) VALUES ('test')");
        assertEquals("test", query1("SELECT name FROM myapp.users"));
    }

    @Test
    void testCreateTableInPublicSchemaExplicit() throws SQLException {
        exec("CREATE TABLE public.items (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO public.items (name) VALUES ('item1')");
        assertEquals("item1", query1("SELECT name FROM public.items"));
    }

    // =========================================================================
    // CHECK constraint with function calls
    // =========================================================================

    @Test
    void testCheckConstraintWithNotEmpty() throws SQLException {
        // Common pattern: CHECK (col <> '')
        exec("CREATE TABLE validated_entries (id text NOT NULL CHECK (id <> '') PRIMARY KEY, name text NOT NULL CHECK (name <> ''))");
        exec("INSERT INTO validated_entries (id, name) VALUES ('abc', 'test')");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO validated_entries (id, name) VALUES ('', 'test')"));
    }

    // =========================================================================
    // INHERITS (table inheritance)
    // =========================================================================

    @Test
    void testTableInheritance() throws SQLException {
        exec("CREATE TABLE base_entity (id serial PRIMARY KEY, created_at timestamp DEFAULT now())");
        exec("CREATE TABLE child_entity (name text NOT NULL) INHERITS (base_entity)");
        exec("INSERT INTO child_entity (name) VALUES ('child1')");
        assertEquals("child1", query1("SELECT name FROM child_entity"));
        // Parent should also see the row
        assertNotNull(query1("SELECT id FROM base_entity"));
    }

    // =========================================================================
    // Array column defaults
    // =========================================================================

    @Test
    void testArrayColumnWithDefault() throws SQLException {
        exec("CREATE TABLE tag_container (id serial PRIMARY KEY, tags text[] DEFAULT '{}'::text[])");
        exec("INSERT INTO tag_container DEFAULT VALUES");
        assertNotNull(query1("SELECT tags FROM tag_container"));
    }

    @Test
    void testJsonbColumnWithObjectDefault() throws SQLException {
        exec("CREATE TABLE config_store (id serial PRIMARY KEY, settings jsonb DEFAULT '{}'::jsonb NOT NULL)");
        exec("INSERT INTO config_store DEFAULT VALUES");
        assertEquals("{}", query1("SELECT settings FROM config_store"));
    }

    // =========================================================================
    // Deferrable constraints
    // =========================================================================

    @Test
    void testDeferrableConstraint() throws SQLException {
        exec("CREATE TABLE parent_defer (id serial PRIMARY KEY)");
        exec("CREATE TABLE child_defer (id serial PRIMARY KEY, parent_id int REFERENCES parent_defer(id) DEFERRABLE INITIALLY DEFERRED)");
        exec("BEGIN");
        // Can insert child before parent within a deferred transaction
        exec("INSERT INTO child_defer (parent_id) VALUES (1)");
        exec("INSERT INTO parent_defer (id) VALUES (1)");
        exec("COMMIT");
        assertEquals("1", query1("SELECT parent_id FROM child_defer"));
    }

    // =========================================================================
    // IF NOT EXISTS with various DDL
    // =========================================================================

    @Test
    void testCreateTableIfNotExistsIsIdempotent() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS idempotent_tbl (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE IF NOT EXISTS idempotent_tbl (id serial PRIMARY KEY, name text)");
        // Should not error
        exec("INSERT INTO idempotent_tbl (name) VALUES ('ok')");
        assertEquals("ok", query1("SELECT name FROM idempotent_tbl"));
    }

    // =========================================================================
    // double precision type in pg_dump
    // =========================================================================

    @Test
    void testDoublePrecisionType() throws SQLException {
        exec("CREATE TABLE fp_test (id serial PRIMARY KEY, val double precision NOT NULL)");
        exec("INSERT INTO fp_test (val) VALUES (3.14159)");
        assertNotNull(query1("SELECT val FROM fp_test"));
    }

    // =========================================================================
    // LIKE / INCLUDING in CREATE TABLE
    // =========================================================================

    @Test
    void testCreateTableLike() throws SQLException {
        exec("CREATE TABLE template_tbl (id serial PRIMARY KEY, name text NOT NULL, created_at timestamp DEFAULT now())");
        exec("CREATE TABLE copy_tbl (LIKE template_tbl INCLUDING ALL)");
        exec("INSERT INTO copy_tbl (name) VALUES ('copied')");
        assertEquals("copied", query1("SELECT name FROM copy_tbl"));
    }

    @Test
    void testCreateTableLikeIncludingDefaults() throws SQLException {
        exec("CREATE TABLE tmpl2 (id serial PRIMARY KEY, status text DEFAULT 'active')");
        exec("CREATE TABLE copy2 (LIKE tmpl2 INCLUDING DEFAULTS)");
        exec("INSERT INTO copy2 DEFAULT VALUES");
        assertEquals("active", query1("SELECT status FROM copy2"));
    }
}
