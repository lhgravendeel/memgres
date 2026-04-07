CREATE TABLE bad_idx (id int, created_at timestamp);
-- expect-error
CREATE INDEX idx_bad ON bad_idx((random()));
