CREATE TABLE bookings (room int, period tsrange);
CREATE EXTENSION IF NOT EXISTS btree_gist;
ALTER TABLE bookings ADD CONSTRAINT no_overlap EXCLUDE USING gist (room WITH =, period WITH &&);
INSERT INTO bookings VALUES (1, tsrange('2024-01-01','2024-01-02'));
-- expect-error
INSERT INTO bookings VALUES (1, tsrange('2024-01-01 12:00','2024-01-03'));
