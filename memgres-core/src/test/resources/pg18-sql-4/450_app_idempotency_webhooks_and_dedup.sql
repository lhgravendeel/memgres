-- 450_app_idempotency_webhooks_and_dedup.sql
-- Idempotency keys, webhook/event deduplication, retry selection.

DROP SCHEMA IF EXISTS test_450 CASCADE;
CREATE SCHEMA test_450;
SET search_path TO test_450;

CREATE TABLE webhook_events (
    id integer PRIMARY KEY,
    source_system text NOT NULL,
    external_event_id text NOT NULL,
    payload jsonb NOT NULL,
    received_at timestamp NOT NULL,
    status text NOT NULL CHECK (status IN ('pending','processing','delivered','failed')),
    attempts integer NOT NULL DEFAULT 0,
    next_retry_at timestamp NULL,
    UNIQUE (source_system, external_event_id)
);

CREATE TABLE idempotency_keys (
    key text PRIMARY KEY,
    request_path text NOT NULL,
    response_code integer NOT NULL,
    created_at timestamp NOT NULL
);

INSERT INTO webhook_events(id, source_system, external_event_id, payload, received_at, status, attempts, next_retry_at) VALUES
(1, 'github', 'evt_1', '{"repo":"a","action":"push"}',   TIMESTAMP '2025-03-01 10:00:00', 'delivered', 1, NULL),
(2, 'github', 'evt_2', '{"repo":"a","action":"issue"}',  TIMESTAMP '2025-03-01 10:05:00', 'failed',    3, TIMESTAMP '2025-03-01 11:00:00'),
(3, 'stripe', 'evt_9', '{"kind":"invoice.paid"}',        TIMESTAMP '2025-03-01 10:10:00', 'pending',   0, TIMESTAMP '2025-03-01 10:15:00'),
(4, 'stripe', 'evt_10','{"kind":"invoice.failed"}',      TIMESTAMP '2025-03-01 10:20:00', 'failed',    1, TIMESTAMP '2025-03-01 10:25:00');

INSERT INTO idempotency_keys(key, request_path, response_code, created_at) VALUES
('idem-1', '/api/orders', 201, TIMESTAMP '2025-03-01 09:00:00'),
('idem-2', '/api/orders', 409, TIMESTAMP '2025-03-01 09:05:00');

-- Detect duplicates by unique external identifiers after ingest.
-- begin-expected
-- columns: source_system,event_count
-- row: github|2
-- row: stripe|2
-- end-expected
SELECT source_system, COUNT(*) AS event_count
FROM webhook_events
GROUP BY source_system
ORDER BY source_system;

-- Retry candidate selection.
-- begin-expected
-- columns: id,source_system,external_event_id,status,attempts
-- row: 2|github|evt_2|failed|3
-- row: 3|stripe|evt_9|pending|0
-- row: 4|stripe|evt_10|failed|1
-- end-expected
SELECT id, source_system, external_event_id, status, attempts
FROM webhook_events
WHERE (status = 'pending')
   OR (status = 'failed' AND next_retry_at <= TIMESTAMP '2025-03-01 11:00:00')
ORDER BY id;

-- Insert duplicate should do nothing under idempotent ingest.
INSERT INTO webhook_events(id, source_system, external_event_id, payload, received_at, status, attempts, next_retry_at)
VALUES (99, 'github', 'evt_1', '{"repo":"a","action":"push"}', TIMESTAMP '2025-03-01 12:00:00', 'pending', 0, NULL)
ON CONFLICT (source_system, external_event_id) DO NOTHING;

-- begin-expected
-- columns: total_events
-- row: 4
-- end-expected
SELECT COUNT(*) AS total_events
FROM webhook_events;

-- Common "have we already handled this request?" query.
-- begin-expected
-- columns: key,response_code
-- row: idem-1|201
-- end-expected
SELECT key, response_code
FROM idempotency_keys
WHERE key = 'idem-1';

-- Mark work as in progress using UPDATE ... RETURNING pattern.
-- begin-expected
-- columns: id,status,attempts
-- row: 3|processing|1
-- end-expected
WITH claimed AS (
    UPDATE webhook_events
    SET status = 'processing',
        attempts = attempts + 1
    WHERE id = 3
      AND status = 'pending'
    RETURNING id, status, attempts
)
SELECT id, status, attempts
FROM claimed;

-- Post-claim summary.
-- begin-expected
-- columns: status,count_rows
-- row: delivered|1
-- row: failed|2
-- row: processing|1
-- end-expected
SELECT status, COUNT(*) AS count_rows
FROM webhook_events
GROUP BY status
ORDER BY status;
