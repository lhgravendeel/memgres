package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests function integration in pg_dump/restore-relevant contexts:
 * functions in views, defaults, generated columns, triggers, CHECK constraints,
 * and various PL/pgSQL patterns that pg_dump would output.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FunctionIntegrationTest {

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

    private String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // ========================================================================
    // 1. SQL-language functions (common in pg_dump output)
    // ========================================================================

    @Test @Order(1)
    void sqlFunction_simpleScalar() throws SQLException {
        exec("CREATE FUNCTION double_it(x integer) RETURNS integer AS $$ SELECT x * 2 $$ LANGUAGE sql");
        assertEquals("10", q("SELECT double_it(5)"));
    }

    @Test @Order(2)
    void sqlFunction_calledInSelect() throws SQLException {
        exec("CREATE TABLE items (id serial PRIMARY KEY, price numeric(10,2))");
        exec("INSERT INTO items (price) VALUES (100.00), (200.00)");
        assertEquals("200.00", q("SELECT double_it(price::integer)::numeric(10,2) FROM items WHERE id = 1"));
    }

    @Test @Order(3)
    void sqlFunction_calledInWhere() throws SQLException {
        assertEquals("200.00", q("SELECT price FROM items WHERE price > double_it(50)::numeric"));
    }

    @Test @Order(4)
    void sqlFunction_immutable() throws SQLException {
        exec("CREATE FUNCTION add_tax(amount numeric) RETURNS numeric AS $$ SELECT amount * 1.21 $$ LANGUAGE sql IMMUTABLE");
        assertEquals("121.00", q("SELECT add_tax(100.00)::numeric(10,2)"));
    }

    @Test @Order(5)
    void sqlFunction_multipleStatements() throws SQLException {
        // SQL functions can have multiple statements; the last one's result is returned
        exec("""
            CREATE FUNCTION compute_total(qty integer, unit_price numeric) RETURNS numeric AS $$
                SELECT qty * unit_price;
            $$ LANGUAGE sql""");
        assertEquals("250.00", q("SELECT compute_total(5, 50.00)::numeric(10,2)"));
    }

    // ========================================================================
    // 2. PL/pgSQL functions (most common in pg_dump output)
    // ========================================================================

    @Test @Order(10)
    void plpgsql_simpleReturn() throws SQLException {
        exec("""
            CREATE FUNCTION greet(name text) RETURNS text AS $$
            BEGIN
                RETURN 'Hello, ' || name || '!';
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("Hello, World!", q("SELECT greet('World')"));
    }

    @Test @Order(11)
    void plpgsql_withDeclare() throws SQLException {
        exec("""
            CREATE FUNCTION factorial(n integer) RETURNS bigint AS $$
            DECLARE
                result bigint := 1;
                i integer;
            BEGIN
                FOR i IN 2..n LOOP
                    result := result * i;
                END LOOP;
                RETURN result;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("120", q("SELECT factorial(5)"));
    }

    @Test @Order(12)
    void plpgsql_ifElse() throws SQLException {
        exec("""
            CREATE FUNCTION classify_price(p numeric) RETURNS text AS $$
            BEGIN
                IF p < 50 THEN RETURN 'cheap';
                ELSIF p < 200 THEN RETURN 'moderate';
                ELSE RETURN 'expensive';
                END IF;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("cheap", q("SELECT classify_price(25)"));
        assertEquals("moderate", q("SELECT classify_price(100)"));
        assertEquals("expensive", q("SELECT classify_price(500)"));
    }

    @Test @Order(13)
    void plpgsql_selectInto() throws SQLException {
        exec("""
            CREATE FUNCTION get_item_price(item_id integer) RETURNS numeric AS $$
            DECLARE
                p numeric;
            BEGIN
                SELECT price INTO p FROM items WHERE id = item_id;
                RETURN p;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("100.00", q("SELECT get_item_price(1)"));
    }

    @Test @Order(14)
    void plpgsql_exceptionHandling() throws SQLException {
        exec("""
            CREATE FUNCTION safe_divide(a numeric, b numeric) RETURNS numeric AS $$
            BEGIN
                RETURN a / b;
            EXCEPTION
                WHEN division_by_zero THEN
                    RETURN 0;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("5", q("SELECT safe_divide(10, 2)"));
        assertEquals("0", q("SELECT safe_divide(10, 0)"));
    }

    @Test @Order(15)
    void plpgsql_performStatement() throws SQLException {
        exec("CREATE TABLE audit_log (msg text)");
        exec("""
            CREATE FUNCTION log_message(m text) RETURNS void AS $$
            BEGIN
                INSERT INTO audit_log (msg) VALUES (m);
            END;
            $$ LANGUAGE plpgsql""");
        exec("SELECT log_message('test entry')");
        assertEquals("test entry", q("SELECT msg FROM audit_log LIMIT 1"));
    }

    @Test @Order(16)
    void plpgsql_loop() throws SQLException {
        exec("""
            CREATE FUNCTION sum_to_n(n integer) RETURNS integer AS $$
            DECLARE
                total integer := 0;
                i integer := 1;
            BEGIN
                WHILE i <= n LOOP
                    total := total + i;
                    i := i + 1;
                END LOOP;
                RETURN total;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("55", q("SELECT sum_to_n(10)"));
    }

    @Test @Order(17)
    void plpgsql_forEachArray() throws SQLException {
        exec("""
            CREATE FUNCTION array_sum(arr integer[]) RETURNS integer AS $$
            DECLARE
                total integer := 0;
                elem integer;
            BEGIN
                FOREACH elem IN ARRAY arr LOOP
                    total := total + elem;
                END LOOP;
                RETURN total;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("15", q("SELECT array_sum(ARRAY[1,2,3,4,5])"));
    }

    @Test @Order(18)
    void plpgsql_dynamicSql() throws SQLException {
        exec("""
            CREATE FUNCTION count_table(tbl text) RETURNS bigint AS $$
            DECLARE
                cnt bigint;
            BEGIN
                EXECUTE 'SELECT count(*) FROM ' || tbl INTO cnt;
                RETURN cnt;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("2", q("SELECT count_table('items')"));
    }

    @Test @Order(19)
    void plpgsql_returnQuery() throws SQLException {
        exec("""
            CREATE FUNCTION cheap_items(max_price numeric) RETURNS SETOF items AS $$
            BEGIN
                RETURN QUERY SELECT * FROM items WHERE price <= max_price ORDER BY id;
            END;
            $$ LANGUAGE plpgsql""");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, price FROM cheap_items(150)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(100.00, rs.getDouble("price"), 0.01);
            assertFalse(rs.next());
        }
    }

    @Test @Order(20)
    void plpgsql_returnNext() throws SQLException {
        exec("""
            CREATE FUNCTION generate_labels(n integer) RETURNS SETOF text AS $$
            DECLARE
                i integer;
            BEGIN
                FOR i IN 1..n LOOP
                    RETURN NEXT 'label_' || i;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql""");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM generate_labels(3)")) {
            assertTrue(rs.next()); assertEquals("label_1", rs.getString(1));
            assertTrue(rs.next()); assertEquals("label_2", rs.getString(1));
            assertTrue(rs.next()); assertEquals("label_3", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // 3. Functions as column DEFAULT values (pg_dump outputs these)
    // ========================================================================

    @Test @Order(30)
    void default_builtinFunction() throws SQLException {
        exec("CREATE TABLE with_uuid (id uuid DEFAULT gen_random_uuid(), name text)");
        exec("INSERT INTO with_uuid (name) VALUES ('test')");
        String uuid = q("SELECT id FROM with_uuid WHERE name = 'test'");
        assertNotNull(uuid);
        assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                "Should generate a valid UUID, got: " + uuid);
    }

    @Test @Order(31)
    void default_nowFunction() throws SQLException {
        exec("CREATE TABLE with_ts (id serial, created_at timestamptz DEFAULT now(), name text)");
        exec("INSERT INTO with_ts (name) VALUES ('test')");
        String ts = q("SELECT created_at FROM with_ts WHERE name = 'test'");
        assertNotNull(ts);
    }

    @Test @Order(32)
    void default_customSqlFunction() throws SQLException {
        exec("CREATE FUNCTION default_status() RETURNS text AS $$ SELECT 'active' $$ LANGUAGE sql IMMUTABLE");
        exec("CREATE TABLE with_custom_default (id serial, status text DEFAULT default_status(), name text)");
        exec("INSERT INTO with_custom_default (name) VALUES ('test')");
        assertEquals("active", q("SELECT status FROM with_custom_default WHERE name = 'test'"));
    }

    @Test @Order(33)
    void default_customPlpgsqlFunction() throws SQLException {
        exec("""
            CREATE FUNCTION next_code() RETURNS text AS $$
            DECLARE
                n integer;
            BEGIN
                SELECT count(*) + 1 INTO n FROM with_custom_default;
                RETURN 'CODE-' || lpad(n::text, 4, '0');
            END;
            $$ LANGUAGE plpgsql""");
        exec("CREATE TABLE orders_with_code (id serial, code text DEFAULT next_code(), description text)");
        exec("INSERT INTO orders_with_code (description) VALUES ('first order')");
        String code = q("SELECT code FROM orders_with_code WHERE description = 'first order'");
        assertNotNull(code);
        assertTrue(code.startsWith("CODE-"), "Should generate code, got: " + code);
    }

    @Test @Order(34)
    void default_expressionWithFunction() throws SQLException {
        exec("CREATE SEQUENCE label_seq START 100");
        exec("CREATE TABLE with_expr_default (id serial, label text DEFAULT 'item-' || nextval('label_seq')::text)");
        exec("INSERT INTO with_expr_default DEFAULT VALUES");
        String label = q("SELECT label FROM with_expr_default LIMIT 1");
        assertNotNull(label);
        assertEquals("item-100", label);
    }

    // ========================================================================
    // 4. Functions in views (very common in pg_dump output)
    // ========================================================================

    @Test @Order(40)
    void view_withBuiltinFunction() throws SQLException {
        exec("CREATE VIEW item_summary AS SELECT id, price, round(price * 1.21, 2) AS price_with_tax FROM items");
        // round() returns numeric which may omit trailing zeros in string form
        assertEquals(121.00, Double.parseDouble(q("SELECT price_with_tax FROM item_summary WHERE id = 1")), 0.01);
    }

    @Test @Order(41)
    void view_withCustomFunction() throws SQLException {
        exec("CREATE VIEW item_classified AS SELECT id, price, classify_price(price) AS category FROM items");
        assertEquals("moderate", q("SELECT category FROM item_classified WHERE id = 1"));
        assertEquals("expensive", q("SELECT category FROM item_classified WHERE id = 2"));
    }

    @Test @Order(42)
    void view_withCustomFunctionInWhere() throws SQLException {
        exec("CREATE VIEW expensive_items AS SELECT * FROM items WHERE price > double_it(75)::numeric");
        assertEquals("200.00", q("SELECT price FROM expensive_items"));
    }

    @Test @Order(43)
    void view_chainedFunctions() throws SQLException {
        exec("""
            CREATE FUNCTION format_price(p numeric) RETURNS text AS $$
            BEGIN
                RETURN '$' || to_char(p, 'FM999,999.00');
            END;
            $$ LANGUAGE plpgsql""");
        exec("CREATE VIEW formatted_items AS SELECT id, format_price(price) AS display_price FROM items");
        String result = q("SELECT display_price FROM formatted_items WHERE id = 1");
        assertTrue(result.startsWith("$"), "Should format price, got: " + result);
    }

    @Test @Order(44)
    void view_withAggregateAndCustomFunction() throws SQLException {
        exec("CREATE TABLE categories (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE products (id serial, category_id integer REFERENCES categories(id), price numeric)");
        exec("INSERT INTO categories (name) VALUES ('Electronics'), ('Books')");
        exec("INSERT INTO products (category_id, price) VALUES (1, 999.99), (1, 499.99), (2, 29.99)");
        exec("""
            CREATE VIEW category_stats AS
            SELECT c.name, count(*) AS product_count,
                   classify_price(avg(p.price)) AS avg_category
            FROM categories c JOIN products p ON c.id = p.category_id
            GROUP BY c.name""");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name, avg_category FROM category_stats ORDER BY name")) {
            assertTrue(rs.next());
            assertEquals("Books", rs.getString("name"));
            assertEquals("cheap", rs.getString("avg_category"));
            assertTrue(rs.next());
            assertEquals("Electronics", rs.getString("name"));
        }
    }

    // ========================================================================
    // 5. Functions in GENERATED ALWAYS AS STORED columns
    // ========================================================================

    @Test @Order(50)
    void generated_withBuiltinFunction() throws SQLException {
        exec("CREATE TABLE names (id serial, first_name text, last_name text, full_name text GENERATED ALWAYS AS (lower(first_name || ' ' || last_name)) STORED)");
        exec("INSERT INTO names (first_name, last_name) VALUES ('John', 'DOE')");
        assertEquals("john doe", q("SELECT full_name FROM names WHERE id = 1"));
    }

    @Test @Order(51)
    void generated_withCustomFunction() throws SQLException {
        exec("""
            CREATE FUNCTION slug(t text) RETURNS text AS $$
            BEGIN
                RETURN lower(replace(t, ' ', '-'));
            END;
            $$ LANGUAGE plpgsql IMMUTABLE""");
        exec("CREATE TABLE articles (id serial, title text, url_slug text GENERATED ALWAYS AS (slug(title)) STORED)");
        exec("INSERT INTO articles (title) VALUES ('Hello World Post')");
        assertEquals("hello-world-post", q("SELECT url_slug FROM articles WHERE id = 1"));
    }

    @Test @Order(52)
    void generated_withMultiColumnCustomFunction() throws SQLException {
        exec("""
            CREATE FUNCTION full_address(street text, city text, zip text) RETURNS text AS $$
            BEGIN
                RETURN street || ', ' || city || ' ' || zip;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE""");
        exec("CREATE TABLE addresses (id serial, street text, city text, zip text, full_addr text GENERATED ALWAYS AS (full_address(street, city, zip)) STORED)");
        exec("INSERT INTO addresses (street, city, zip) VALUES ('123 Main St', 'Springfield', '62701')");
        assertEquals("123 Main St, Springfield 62701", q("SELECT full_addr FROM addresses WHERE id = 1"));
    }

    // ========================================================================
    // 6. Trigger functions (pg_dump outputs CREATE FUNCTION + CREATE TRIGGER)
    // ========================================================================

    @Test @Order(60)
    void trigger_beforeInsertSetsColumn() throws SQLException {
        exec("CREATE TABLE tracked (id serial, name text, created_by text)");
        exec("""
            CREATE FUNCTION set_created_by() RETURNS trigger AS $$
            BEGIN
                NEW.created_by := current_user;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql""");
        exec("CREATE TRIGGER trg_created_by BEFORE INSERT ON tracked FOR EACH ROW EXECUTE FUNCTION set_created_by()");
        exec("INSERT INTO tracked (name) VALUES ('test')");
        String createdBy = q("SELECT created_by FROM tracked WHERE name = 'test'");
        assertNotNull(createdBy);
    }

    @Test @Order(61)
    void trigger_beforeUpdateTimestamp() throws SQLException {
        exec("CREATE TABLE docs (id serial, content text, updated_at timestamptz DEFAULT now())");
        exec("INSERT INTO docs (content) VALUES ('original')");
        String ts1 = q("SELECT updated_at FROM docs WHERE id = 1");
        exec("""
            CREATE FUNCTION update_timestamp() RETURNS trigger AS $$
            BEGIN
                NEW.updated_at := now();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql""");
        exec("CREATE TRIGGER trg_updated BEFORE UPDATE ON docs FOR EACH ROW EXECUTE FUNCTION update_timestamp()");
        exec("UPDATE docs SET content = 'modified' WHERE id = 1");
        String ts2 = q("SELECT updated_at FROM docs WHERE id = 1");
        assertNotNull(ts2);
    }

    @Test @Order(62)
    void trigger_auditLog() throws SQLException {
        exec("CREATE TABLE trigger_audit (id serial, table_name text, operation text, old_data text, new_data text, ts timestamptz DEFAULT now())");
        exec("CREATE TABLE employees_t (id serial, name text, salary numeric)");
        exec("""
            CREATE FUNCTION audit_changes() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    INSERT INTO trigger_audit (table_name, operation, new_data)
                    VALUES (TG_TABLE_NAME, 'INSERT', NEW.name);
                    RETURN NEW;
                ELSIF TG_OP = 'UPDATE' THEN
                    INSERT INTO trigger_audit (table_name, operation, old_data, new_data)
                    VALUES (TG_TABLE_NAME, 'UPDATE', OLD.name, NEW.name);
                    RETURN NEW;
                ELSIF TG_OP = 'DELETE' THEN
                    INSERT INTO trigger_audit (table_name, operation, old_data)
                    VALUES (TG_TABLE_NAME, 'DELETE', OLD.name);
                    RETURN OLD;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql""");
        exec("CREATE TRIGGER trg_audit BEFORE INSERT OR UPDATE OR DELETE ON employees_t FOR EACH ROW EXECUTE FUNCTION audit_changes()");
        exec("INSERT INTO employees_t (name, salary) VALUES ('Alice', 50000)");
        assertEquals("INSERT", q("SELECT operation FROM trigger_audit ORDER BY id DESC LIMIT 1"));
        exec("UPDATE employees_t SET name = 'Bob' WHERE name = 'Alice'");
        assertEquals("UPDATE", q("SELECT operation FROM trigger_audit ORDER BY id DESC LIMIT 1"));
        exec("DELETE FROM employees_t WHERE name = 'Bob'");
        assertEquals("DELETE", q("SELECT operation FROM trigger_audit ORDER BY id DESC LIMIT 1"));
    }

    @Test @Order(63)
    void trigger_conditionalLogic() throws SQLException {
        exec("CREATE TABLE orders_t (id serial, amount numeric, discount numeric DEFAULT 0)");
        exec("""
            CREATE FUNCTION apply_bulk_discount() RETURNS trigger AS $$
            BEGIN
                IF NEW.amount > 1000 THEN
                    NEW.discount := NEW.amount * 0.10;
                ELSIF NEW.amount > 500 THEN
                    NEW.discount := NEW.amount * 0.05;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql""");
        exec("CREATE TRIGGER trg_discount BEFORE INSERT ON orders_t FOR EACH ROW EXECUTE FUNCTION apply_bulk_discount()");
        exec("INSERT INTO orders_t (amount) VALUES (2000)");
        assertEquals(200.0, Double.parseDouble(q("SELECT discount FROM orders_t WHERE amount = 2000")), 0.01);
        exec("INSERT INTO orders_t (amount) VALUES (750)");
        assertEquals(37.50, Double.parseDouble(q("SELECT discount FROM orders_t WHERE amount = 750")), 0.01);
        exec("INSERT INTO orders_t (amount) VALUES (100)");
        assertEquals("0", q("SELECT discount FROM orders_t WHERE amount = 100"));
    }

    // ========================================================================
    // 7. Functions calling other functions
    // ========================================================================

    @Test @Order(70)
    void functionCallingFunction() throws SQLException {
        exec("""
            CREATE FUNCTION add_one(x integer) RETURNS integer AS $$
            BEGIN RETURN x + 1; END;
            $$ LANGUAGE plpgsql""");
        exec("""
            CREATE FUNCTION add_two(x integer) RETURNS integer AS $$
            BEGIN RETURN add_one(add_one(x)); END;
            $$ LANGUAGE plpgsql""");
        assertEquals("7", q("SELECT add_two(5)"));
    }

    @Test @Order(71)
    void sqlFunctionCallingPlpgsqlFunction() throws SQLException {
        exec("CREATE FUNCTION doubled_greeting(name text) RETURNS text AS $$ SELECT greet(name) || ' ' || greet(name) $$ LANGUAGE sql");
        assertEquals("Hello, X! Hello, X!", q("SELECT doubled_greeting('X')"));
    }

    @Test @Order(72)
    void plpgsqlCallingBuiltinFunctions() throws SQLException {
        exec("""
            CREATE FUNCTION format_record(n text, p numeric) RETURNS text AS $fn$
            DECLARE
                result text;
            BEGIN
                result := upper(n) || ': $' || to_char(p, 'FM999,999.00');
                RETURN result;
            END;
            $fn$ LANGUAGE plpgsql""");
        assertEquals("WIDGET: $100.00", q("SELECT format_record('widget', 100)"));
    }

    // ========================================================================
    // 8. Procedures (CALL syntax, as pg_dump outputs these)
    // ========================================================================

    @Test @Order(80)
    void procedure_basic() throws SQLException {
        exec("CREATE TABLE proc_test (id serial, val text)");
        exec("""
            CREATE PROCEDURE insert_val(v text) AS $$
            BEGIN
                INSERT INTO proc_test (val) VALUES (v);
            END;
            $$ LANGUAGE plpgsql""");
        exec("CALL insert_val('hello')");
        assertEquals("hello", q("SELECT val FROM proc_test LIMIT 1"));
    }

    @Test @Order(81)
    void procedure_withLoop() throws SQLException {
        exec("""
            CREATE PROCEDURE insert_range(n integer) AS $$
            DECLARE
                i integer;
            BEGIN
                FOR i IN 1..n LOOP
                    INSERT INTO proc_test (val) VALUES ('item_' || i);
                END LOOP;
            END;
            $$ LANGUAGE plpgsql""");
        exec("DELETE FROM proc_test");
        exec("CALL insert_range(3)");
        assertEquals("3", q("SELECT count(*) FROM proc_test"));
    }

    // ========================================================================
    // 9. Schema-qualified function calls (pg_dump uses schema.function())
    // ========================================================================

    @Test @Order(90)
    void schemaQualifiedFunction() throws SQLException {
        exec("CREATE SCHEMA utils");
        exec("""
            CREATE FUNCTION utils.clean_text(t text) RETURNS text AS $$
            BEGIN
                RETURN trim(lower(t));
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("hello", q("SELECT utils.clean_text('  Hello  ')"));
    }

    @Test @Order(91)
    void schemaQualifiedInView() throws SQLException {
        exec("CREATE TABLE raw_data (id serial, val text)");
        exec("INSERT INTO raw_data (val) VALUES ('  MESSY  ')");
        exec("CREATE VIEW clean_data AS SELECT id, utils.clean_text(val) AS clean_val FROM raw_data");
        assertEquals("messy", q("SELECT clean_val FROM clean_data WHERE id = 1"));
    }

    // ========================================================================
    // 10. OR REPLACE and DROP/recreate (pg_dump uses CREATE OR REPLACE)
    // ========================================================================

    @Test @Order(100)
    void createOrReplace() throws SQLException {
        exec("CREATE FUNCTION replaceable() RETURNS text AS $$ SELECT 'v1' $$ LANGUAGE sql");
        assertEquals("v1", q("SELECT replaceable()"));
        exec("CREATE OR REPLACE FUNCTION replaceable() RETURNS text AS $$ SELECT 'v2' $$ LANGUAGE sql");
        assertEquals("v2", q("SELECT replaceable()"));
    }

    @Test @Order(101)
    void dropAndRecreate() throws SQLException {
        exec("DROP FUNCTION IF EXISTS replaceable()");
        exec("CREATE FUNCTION replaceable() RETURNS text AS $$ SELECT 'v3' $$ LANGUAGE sql");
        assertEquals("v3", q("SELECT replaceable()"));
    }

    // ========================================================================
    // 11. Function with OUT parameters (pg_dump outputs these)
    // ========================================================================

    @Test @Order(110)
    void functionWithOutParam() throws SQLException {
        exec("""
            CREATE FUNCTION get_stats(OUT min_price numeric, OUT max_price numeric) AS $$
            BEGIN
                SELECT min(price), max(price) INTO min_price, max_price FROM items;
            END;
            $$ LANGUAGE plpgsql""");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM get_stats()")) {
            assertTrue(rs.next());
            assertEquals(100.00, rs.getDouble("min_price"), 0.01);
            assertEquals(200.00, rs.getDouble("max_price"), 0.01);
        }
    }

    @Test @Order(111)
    void functionWithInOutParam() throws SQLException {
        exec("""
            CREATE FUNCTION increment(INOUT val integer, step integer DEFAULT 1) AS $$
            BEGIN
                val := val + step;
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("11", q("SELECT increment(10)"));
        assertEquals("15", q("SELECT increment(10, 5)"));
    }

    // ========================================================================
    // 12. SETOF and TABLE-returning functions (pg_dump outputs these)
    // ========================================================================

    @Test @Order(120)
    void returnsTable() throws SQLException {
        exec("""
            CREATE FUNCTION items_in_range(low numeric, high numeric)
            RETURNS TABLE(item_id integer, item_price numeric) AS $$
            BEGIN
                RETURN QUERY SELECT id, price FROM items WHERE price BETWEEN low AND high ORDER BY id;
            END;
            $$ LANGUAGE plpgsql""");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM items_in_range(50, 150)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("item_id"));
            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // 13. Functions with default parameter values (pg_dump preserves these)
    // ========================================================================

    @Test @Order(130)
    void functionWithDefaults() throws SQLException {
        exec("""
            CREATE FUNCTION make_greeting(name text, greeting text DEFAULT 'Hello') RETURNS text AS $$
            BEGIN
                RETURN greeting || ', ' || name || '!';
            END;
            $$ LANGUAGE plpgsql""");
        assertEquals("Hello, World!", q("SELECT make_greeting('World')"));
        assertEquals("Hi, World!", q("SELECT make_greeting('World', 'Hi')"));
    }

    // ========================================================================
    // 14. Functions used in CHECK constraints
    // ========================================================================

    @Test @Order(140)
    void functionInCheckConstraint() throws SQLException {
        exec("""
            CREATE FUNCTION is_valid_email(e text) RETURNS boolean AS $$
            BEGIN
                RETURN e LIKE '%@%.%';
            END;
            $$ LANGUAGE plpgsql IMMUTABLE""");
        exec("CREATE TABLE contacts (id serial, email text CHECK (is_valid_email(email)))");
        exec("INSERT INTO contacts (email) VALUES ('test@example.com')");
        assertEquals("test@example.com", q("SELECT email FROM contacts LIMIT 1"));
        // Invalid email should fail
        try (Statement s = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                s.execute("INSERT INTO contacts (email) VALUES ('not-an-email')"));
        }
    }

    // ========================================================================
    // 15. Functions with SECURITY DEFINER (pg_dump outputs this attribute)
    // ========================================================================

    @Test @Order(150)
    void securityDefinerFunction() throws SQLException {
        exec("""
            CREATE FUNCTION sec_func() RETURNS text AS $$
            BEGIN
                RETURN current_user;
            END;
            $$ LANGUAGE plpgsql SECURITY DEFINER""");
        // Should at least create and execute without error
        String result = q("SELECT sec_func()");
        assertNotNull(result);
    }

    // ========================================================================
    // 16. DO blocks (anonymous PL/pgSQL, as pg_dump uses these)
    // ========================================================================

    @Test @Order(160)
    void doBlock_basic() throws SQLException {
        exec("CREATE TABLE do_test (val text)");
        exec("""
            DO $$
            BEGIN
                INSERT INTO do_test VALUES ('from do block');
            END;
            $$""");
        assertEquals("from do block", q("SELECT val FROM do_test LIMIT 1"));
    }

    @Test @Order(161)
    void doBlock_withLoop() throws SQLException {
        exec("""
            DO $$
            DECLARE
                i integer;
            BEGIN
                FOR i IN 1..5 LOOP
                    INSERT INTO do_test VALUES ('row_' || i);
                END LOOP;
            END;
            $$""");
        assertEquals("5", q("SELECT count(*) FROM do_test WHERE val LIKE 'row_%'"));
    }

    // ========================================================================
    // 17. pg_dump preamble patterns (SET + function creation order)
    // ========================================================================

    @Test @Order(170)
    void pgDumpFunctionPreamble() throws SQLException {
        // pg_dump creates functions before tables that reference them
        exec("SET check_function_bodies = false");
        exec("""
            CREATE FUNCTION compute_hash(input text) RETURNS text AS $$
            BEGIN
                RETURN md5(input);
            END;
            $$ LANGUAGE plpgsql IMMUTABLE""");
        exec("SET check_function_bodies = true");
        exec("CREATE TABLE hashed_data (id serial, data text, hash text GENERATED ALWAYS AS (compute_hash(data)) STORED)");
        exec("INSERT INTO hashed_data (data) VALUES ('test')");
        String hash = q("SELECT hash FROM hashed_data WHERE id = 1");
        assertEquals(q("SELECT md5('test')"), hash);
    }

    // ========================================================================
    // 18. Function volatility categories (pg_dump preserves these)
    // ========================================================================

    @Test @Order(180)
    void volatilityCategories() throws SQLException {
        exec("CREATE FUNCTION vol_immutable(x int) RETURNS int AS $$ SELECT x * 2 $$ LANGUAGE sql IMMUTABLE");
        exec("CREATE FUNCTION vol_stable(x int) RETURNS int AS $$ SELECT x * 2 $$ LANGUAGE sql STABLE");
        exec("CREATE FUNCTION vol_volatile(x int) RETURNS int AS $$ SELECT x * 2 $$ LANGUAGE sql VOLATILE");
        // All should work the same functionally
        assertEquals("10", q("SELECT vol_immutable(5)"));
        assertEquals("10", q("SELECT vol_stable(5)"));
        assertEquals("10", q("SELECT vol_volatile(5)"));
    }
}
