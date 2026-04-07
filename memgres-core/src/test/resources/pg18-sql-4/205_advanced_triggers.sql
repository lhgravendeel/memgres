DROP SCHEMA IF EXISTS test_205 CASCADE;
CREATE SCHEMA test_205;
SET search_path TO test_205;

CREATE TABLE base_orders (
    order_id integer PRIMARY KEY,
    customer text NOT NULL,
    amount integer NOT NULL,
    status text NOT NULL DEFAULT 'new'
);

CREATE TABLE order_audit (
    audit_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action text NOT NULL,
    row_count integer NOT NULL,
    summary text NOT NULL
);

CREATE TABLE row_audit (
    audit_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id integer NOT NULL,
    action text NOT NULL,
    old_status text,
    new_status text
);

CREATE OR REPLACE FUNCTION row_audit_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO row_audit(order_id, action, old_status, new_status)
        VALUES (NEW.order_id, TG_OP, OLD.status, NEW.status);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO row_audit(order_id, action, old_status, new_status)
        VALUES (OLD.order_id, TG_OP, OLD.status, NULL);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION stmt_audit_insert_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count integer;
    inserted_sum integer;
BEGIN
    SELECT count(*), COALESCE(sum(amount), 0)
    INTO inserted_count, inserted_sum
    FROM new_rows;

    INSERT INTO order_audit(action, row_count, summary)
    VALUES ('INSERT', inserted_count, 'sum=' || inserted_sum::text);
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_row_audit_update
AFTER UPDATE OF status ON base_orders
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION row_audit_fn();

CREATE TRIGGER trg_row_audit_delete
AFTER DELETE ON base_orders
FOR EACH ROW
EXECUTE FUNCTION row_audit_fn();

CREATE TRIGGER trg_stmt_insert
AFTER INSERT ON base_orders
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION stmt_audit_insert_fn();

INSERT INTO base_orders(order_id, customer, amount, status) VALUES
    (1, 'anna', 100, 'new'),
    (2, 'bob', 200, 'new');

UPDATE base_orders
SET status = 'paid'
WHERE order_id = 1;

DELETE FROM base_orders
WHERE order_id = 2;

-- begin-expected
-- columns: action|row_count|summary
-- row: INSERT|2|sum=300
-- end-expected
SELECT action, row_count, summary
FROM order_audit
ORDER BY audit_id;

-- begin-expected
-- columns: order_id|action|old_status|new_status
-- row: 1|UPDATE|new|paid
-- row: 2|DELETE|new|
-- end-expected
SELECT order_id, action, old_status, new_status
FROM row_audit
ORDER BY audit_id;

CREATE VIEW order_entry AS
SELECT order_id, customer, amount, status
FROM base_orders;

CREATE OR REPLACE FUNCTION order_entry_ins_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO base_orders(order_id, customer, amount, status)
    VALUES (NEW.order_id, upper(NEW.customer), NEW.amount, COALESCE(NEW.status, 'new'));
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_view_insert
INSTEAD OF INSERT ON order_entry
FOR EACH ROW
EXECUTE FUNCTION order_entry_ins_fn();

INSERT INTO order_entry(order_id, customer, amount, status)
VALUES (3, 'cara', 150, NULL);

-- begin-expected
-- columns: order_id|customer|amount|status
-- row: 1|anna|100|paid
-- row: 3|CARA|150|new
-- end-expected
SELECT order_id, customer, amount, status
FROM base_orders
ORDER BY order_id;

DROP SCHEMA test_205 CASCADE;
