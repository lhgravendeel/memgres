CREATE TABLE users (id int, email text, deleted_at timestamp);
CREATE UNIQUE INDEX idx_users_email_active ON users(lower(email)) WHERE deleted_at IS NULL;
INSERT INTO users VALUES (1,'a@test.com',NULL),(2,'a@test.com','2024-01-01');
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT COUNT(*) FROM users;
