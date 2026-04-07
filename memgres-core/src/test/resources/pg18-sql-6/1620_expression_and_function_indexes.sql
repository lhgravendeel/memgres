CREATE TABLE expr_test (email text, payload jsonb);
CREATE INDEX idx_lower_email ON expr_test(lower(email));
CREATE INDEX idx_json_sku ON expr_test((payload->>'sku'));
INSERT INTO expr_test VALUES ('Alice@test.com','{"sku":"A1"}'),('alice@test.com','{"sku":"A2"}');
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT COUNT(*) FROM expr_test;
