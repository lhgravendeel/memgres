-- ============================================================
-- 09: Views, Functions, Triggers, and Sequences
-- ============================================================

-- Setup
CREATE TABLE products (id serial PRIMARY KEY, name text NOT NULL, category text, price numeric(10,2), active boolean DEFAULT true);
INSERT INTO products (name, category, price) VALUES
  ('Widget', 'hardware', 9.99), ('Gadget', 'electronics', 19.99),
  ('Tool', 'hardware', 14.99), ('Service', 'services', 49.99),
  ('Gizmo', 'electronics', 29.99), ('Part', 'hardware', 4.99);

-- === CREATE VIEW ===
CREATE VIEW active_products AS SELECT * FROM products WHERE active = true;
SELECT * FROM active_products ORDER BY name;
SELECT COUNT(*) FROM active_products;

CREATE VIEW category_stats AS
SELECT category, COUNT(*) AS cnt, AVG(price)::numeric(10,2) AS avg_price, SUM(price) AS total
FROM products WHERE active = true GROUP BY category;
SELECT * FROM category_stats ORDER BY category;

-- === CREATE OR REPLACE VIEW ===
CREATE OR REPLACE VIEW active_products AS SELECT id, name, price FROM products WHERE active = true;
SELECT * FROM active_products ORDER BY name;

-- === DROP VIEW ===
CREATE VIEW temp_view AS SELECT 1 AS n;
DROP VIEW temp_view;
DROP VIEW IF EXISTS temp_view;

-- === Materialized view ===
CREATE MATERIALIZED VIEW product_summary AS
SELECT category, COUNT(*) AS cnt, SUM(price) AS total FROM products GROUP BY category;
SELECT * FROM product_summary ORDER BY category;
INSERT INTO products (name, category, price) VALUES ('NewItem', 'hardware', 7.99);
SELECT * FROM product_summary ORDER BY category;
REFRESH MATERIALIZED VIEW product_summary;
SELECT * FROM product_summary ORDER BY category;
DROP MATERIALIZED VIEW product_summary;

-- === Sequences ===
CREATE SEQUENCE my_counter START WITH 100 INCREMENT BY 5;
SELECT nextval('my_counter');
SELECT nextval('my_counter');
SELECT nextval('my_counter');
SELECT currval('my_counter');
SELECT setval('my_counter', 200);
SELECT nextval('my_counter');
SELECT setval('my_counter', 300, false);
SELECT nextval('my_counter');
ALTER SEQUENCE my_counter RESTART WITH 1;
SELECT nextval('my_counter');
DROP SEQUENCE my_counter;

-- === PL/pgSQL functions ===
CREATE FUNCTION add_numbers(a int, b int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RETURN a + b;
END;
$$;
SELECT add_numbers(3, 7);
SELECT add_numbers(0, 0);
SELECT add_numbers(-5, 5);

CREATE FUNCTION greet(name text, greeting text DEFAULT 'Hello') RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  RETURN greeting || ', ' || name || '!';
END;
$$;
SELECT greet('World');
SELECT greet('World', 'Hi');

CREATE FUNCTION factorial(n int) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE result bigint := 1;
BEGIN
  FOR i IN 2..n LOOP
    result := result * i;
  END LOOP;
  RETURN result;
END;
$$;
SELECT factorial(0);
SELECT factorial(1);
SELECT factorial(5);
SELECT factorial(10);

-- === SQL language function ===
CREATE FUNCTION double_price(p numeric) RETURNS numeric LANGUAGE sql AS $$
  SELECT p * 2;
$$;
SELECT double_price(9.99);

-- === Function returning SETOF ===
CREATE FUNCTION cheap_products(max_price numeric) RETURNS SETOF products LANGUAGE sql AS $$
  SELECT * FROM products WHERE price <= max_price ORDER BY price;
$$;
SELECT name, price FROM cheap_products(15.00);

-- === Function returning TABLE ===
CREATE FUNCTION product_stats_fn() RETURNS TABLE(cat text, cnt bigint, avg_price numeric) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT category, COUNT(*), AVG(price)::numeric(10,2) FROM products GROUP BY category ORDER BY category;
END;
$$;
SELECT * FROM product_stats_fn();

-- === DO blocks ===
DO $$ BEGIN RAISE NOTICE 'Hello from DO block'; END; $$;

DO $$
DECLARE cnt int;
BEGIN
  SELECT COUNT(*) INTO cnt FROM products;
  IF cnt > 5 THEN
    RAISE NOTICE 'Many products: %', cnt;
  END IF;
END;
$$;

-- === Triggers ===
CREATE TABLE audit_log (id serial PRIMARY KEY, action text, table_name text, record_id int, ts timestamp DEFAULT now());
CREATE FUNCTION log_product_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO audit_log (action, table_name, record_id) VALUES ('INSERT', TG_TABLE_NAME, NEW.id);
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO audit_log (action, table_name, record_id) VALUES ('UPDATE', TG_TABLE_NAME, NEW.id);
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO audit_log (action, table_name, record_id) VALUES ('DELETE', TG_TABLE_NAME, OLD.id);
    RETURN OLD;
  END IF;
END;
$$;

CREATE TRIGGER product_audit
AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW EXECUTE FUNCTION log_product_change();

INSERT INTO products (name, category, price) VALUES ('Triggered', 'test', 1.00);
UPDATE products SET price = 2.00 WHERE name = 'Triggered';
DELETE FROM products WHERE name = 'Triggered';
SELECT action, record_id FROM audit_log ORDER BY id;

-- === Updated_at trigger ===
CREATE TABLE auto_updated (id serial PRIMARY KEY, val text, updated_at timestamp DEFAULT now());
CREATE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;
CREATE TRIGGER auto_update_trigger BEFORE UPDATE ON auto_updated FOR EACH ROW EXECUTE FUNCTION set_updated_at();
INSERT INTO auto_updated (val) VALUES ('original');
SELECT val FROM auto_updated;
UPDATE auto_updated SET val = 'modified' WHERE id = 1;
SELECT val FROM auto_updated;

-- === ENUM types ===
CREATE TYPE status_type AS ENUM ('draft', 'published', 'archived');
CREATE TABLE articles (id serial PRIMARY KEY, title text, status status_type DEFAULT 'draft');
INSERT INTO articles (title) VALUES ('My Article');
INSERT INTO articles (title, status) VALUES ('Published', 'published');
SELECT * FROM articles ORDER BY id;
INSERT INTO articles (title, status) VALUES ('Bad Status', 'invalid_status');
ALTER TYPE status_type ADD VALUE 'deleted';
INSERT INTO articles (title, status) VALUES ('Deleted', 'deleted');
SELECT * FROM articles ORDER BY id;

-- === DROP FUNCTION ===
DROP FUNCTION add_numbers(int, int);
DROP FUNCTION IF EXISTS nonexistent_func();

-- === INVALID ===
-- View referencing nonexistent table
CREATE VIEW bad_view AS SELECT * FROM nonexistent_xyz;
-- Function with body error
CREATE FUNCTION bad_fn() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 'not_a_number'; END; $$;

-- Cleanup
DROP TRIGGER product_audit ON products;
DROP TRIGGER auto_update_trigger ON auto_updated;
DROP FUNCTION log_product_change();
DROP FUNCTION set_updated_at();
DROP FUNCTION greet(text, text);
DROP FUNCTION factorial(int);
DROP FUNCTION double_price(numeric);
DROP FUNCTION cheap_products(numeric);
DROP FUNCTION product_stats_fn();
DROP TABLE audit_log;
DROP TABLE auto_updated;
DROP TABLE articles;
DROP TYPE status_type;
DROP VIEW active_products;
DROP VIEW category_stats;
DROP TABLE products;
