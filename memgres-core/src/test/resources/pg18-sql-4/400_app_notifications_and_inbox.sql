-- 400_app_notifications_and_inbox.sql
-- Notification/inbox query patterns.

DROP SCHEMA IF EXISTS test_400 CASCADE;
CREATE SCHEMA test_400;
SET search_path TO test_400;

CREATE TABLE users (
    id integer PRIMARY KEY,
    username text NOT NULL UNIQUE
);

CREATE TABLE notifications (
    id integer PRIMARY KEY,
    user_id integer NOT NULL REFERENCES users(id),
    actor_user_id integer NOT NULL REFERENCES users(id),
    kind text NOT NULL,
    object_type text NOT NULL,
    object_id integer NOT NULL,
    created_at timestamp NOT NULL,
    read_at timestamp NULL,
    archived_at timestamp NULL
);

CREATE TABLE notification_preferences (
    user_id integer PRIMARY KEY REFERENCES users(id),
    email_enabled boolean NOT NULL,
    mention_enabled boolean NOT NULL,
    watch_enabled boolean NOT NULL
);

INSERT INTO users(id, username) VALUES
(1,'alice'),
(2,'bob'),
(3,'carol'),
(4,'dave');

INSERT INTO notification_preferences(user_id, email_enabled, mention_enabled, watch_enabled) VALUES
(1, true, true, true),
(2, true, false, true),
(3, false, true, false),
(4, true, true, false);

INSERT INTO notifications(id, user_id, actor_user_id, kind, object_type, object_id, created_at, read_at, archived_at) VALUES
(1, 1, 2, 'mention', 'issue', 10, TIMESTAMP '2025-01-01 10:00:00', NULL, NULL),
(2, 1, 3, 'watch',   'issue', 11, TIMESTAMP '2025-01-01 11:00:00', TIMESTAMP '2025-01-01 12:00:00', NULL),
(3, 1, 4, 'assign',  'task',  20, TIMESTAMP '2025-01-02 09:00:00', NULL, NULL),
(4, 1, 2, 'watch',   'merge', 30, TIMESTAMP '2025-01-03 09:00:00', NULL, TIMESTAMP '2025-01-04 00:00:00'),
(5, 2, 1, 'mention', 'issue', 12, TIMESTAMP '2025-01-01 09:00:00', NULL, NULL),
(6, 2, 3, 'watch',   'task',  21, TIMESTAMP '2025-01-02 14:00:00', NULL, NULL),
(7, 2, 4, 'assign',  'task',  22, TIMESTAMP '2025-01-03 15:00:00', TIMESTAMP '2025-01-03 16:00:00', NULL),
(8, 3, 1, 'mention', 'doc',   40, TIMESTAMP '2025-01-04 10:00:00', NULL, NULL),
(9, 3, 2, 'watch',   'issue', 13, TIMESTAMP '2025-01-05 10:00:00', NULL, NULL),
(10,4, 1, 'assign',  'task',  23, TIMESTAMP '2025-01-02 08:30:00', NULL, NULL);

-- Unread counts per user for inbox badge queries.
-- begin-expected
-- columns: username,unread_count
-- row: alice|2
-- row: bob|2
-- row: carol|2
-- row: dave|1
-- end-expected
SELECT u.username, COUNT(*) AS unread_count
FROM users u
JOIN notifications n ON n.user_id = u.id
WHERE n.read_at IS NULL
  AND n.archived_at IS NULL
GROUP BY u.username
ORDER BY u.username;

-- Latest active inbox notification per user.
WITH ranked AS (
    SELECT
        n.*,
        ROW_NUMBER() OVER (PARTITION BY n.user_id ORDER BY n.created_at DESC, n.id DESC) AS rn
    FROM notifications n
    WHERE n.archived_at IS NULL
)
-- begin-expected
-- columns: username,kind,object_type,object_id,created_at
-- row: alice|assign|task|20|2025-01-02 09:00:00
-- row: bob|assign|task|22|2025-01-03 15:00:00
-- row: carol|watch|issue|13|2025-01-05 10:00:00
-- row: dave|assign|task|23|2025-01-02 08:30:00
-- end-expected
SELECT u.username, r.kind, r.object_type, r.object_id, r.created_at
FROM ranked r
JOIN users u ON u.id = r.user_id
WHERE r.rn = 1
ORDER BY u.username;

-- Notifications that should generate email after applying per-kind preferences.
WITH emailable AS (
    SELECT
        u.username,
        n.id,
        n.kind
    FROM notifications n
    JOIN users u ON u.id = n.user_id
    JOIN notification_preferences p ON p.user_id = n.user_id
    WHERE n.archived_at IS NULL
      AND n.read_at IS NULL
      AND (
          (n.kind = 'mention' AND p.email_enabled AND p.mention_enabled) OR
          (n.kind = 'watch'   AND p.email_enabled AND p.watch_enabled) OR
          (n.kind = 'assign'  AND p.email_enabled)
      )
)
-- begin-expected
-- columns: username,id,kind
-- row: alice|1|mention
-- row: alice|3|assign
-- row: bob|6|watch
-- row: dave|10|assign
-- end-expected
SELECT username, id, kind
FROM emailable
ORDER BY username, id;

-- "Has unread" flags and unread mention counts.
-- begin-expected
-- columns: username,has_unread,unread_mentions
-- row: alice|t|1
-- row: bob|t|1
-- row: carol|t|1
-- row: dave|t|0
-- end-expected
SELECT
    u.username,
    EXISTS (
        SELECT 1
        FROM notifications n
        WHERE n.user_id = u.id
          AND n.read_at IS NULL
          AND n.archived_at IS NULL
    ) AS has_unread,
    COUNT(*) FILTER (
        WHERE n.read_at IS NULL
          AND n.archived_at IS NULL
          AND n.kind = 'mention'
    ) AS unread_mentions
FROM users u
LEFT JOIN notifications n ON n.user_id = u.id
GROUP BY u.username
ORDER BY u.username;

-- Bulk mark-as-read pattern.
UPDATE notifications
SET read_at = TIMESTAMP '2025-01-06 09:00:00'
WHERE user_id = 2
  AND read_at IS NULL
  AND archived_at IS NULL;

-- begin-expected
-- columns: username,unread_count
-- row: alice|2
-- row: bob|1
-- row: carol|2
-- row: dave|1
-- end-expected
SELECT u.username, COUNT(*) AS unread_count
FROM users u
LEFT JOIN notifications n
  ON n.user_id = u.id
 AND n.read_at IS NULL
 AND n.archived_at IS NULL
GROUP BY u.username
ORDER BY u.username;
