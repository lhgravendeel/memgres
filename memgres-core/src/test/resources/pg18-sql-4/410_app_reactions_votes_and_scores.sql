-- 410_app_reactions_votes_and_scores.sql
-- Reactions, votes, scores, and current-user reaction query patterns.

DROP SCHEMA IF EXISTS test_410 CASCADE;
CREATE SCHEMA test_410;
SET search_path TO test_410;

CREATE TABLE users (
    id integer PRIMARY KEY,
    username text NOT NULL UNIQUE
);

CREATE TABLE posts (
    id integer PRIMARY KEY,
    title text NOT NULL,
    created_at timestamp NOT NULL
);

CREATE TABLE reactions (
    post_id integer NOT NULL REFERENCES posts(id),
    user_id integer NOT NULL REFERENCES users(id),
    reaction_type text NOT NULL CHECK (reaction_type IN ('like','love','laugh','up','down')),
    created_at timestamp NOT NULL,
    PRIMARY KEY (post_id, user_id, reaction_type)
);

INSERT INTO users(id, username) VALUES
(1,'alice'),
(2,'bob'),
(3,'carol'),
(4,'dave'),
(5,'erin');

INSERT INTO posts(id, title, created_at) VALUES
(10,'First Post',  TIMESTAMP '2025-02-01 10:00:00'),
(11,'Second Post', TIMESTAMP '2025-02-01 11:00:00'),
(12,'Third Post',  TIMESTAMP '2025-02-02 09:00:00');

INSERT INTO reactions(post_id, user_id, reaction_type, created_at) VALUES
(10,1,'up',   TIMESTAMP '2025-02-01 10:05:00'),
(10,2,'up',   TIMESTAMP '2025-02-01 10:06:00'),
(10,3,'down', TIMESTAMP '2025-02-01 10:07:00'),
(10,4,'like', TIMESTAMP '2025-02-01 10:08:00'),
(11,1,'up',   TIMESTAMP '2025-02-01 11:05:00'),
(11,2,'up',   TIMESTAMP '2025-02-01 11:06:00'),
(11,3,'up',   TIMESTAMP '2025-02-01 11:07:00'),
(11,4,'love', TIMESTAMP '2025-02-01 11:08:00'),
(12,2,'down', TIMESTAMP '2025-02-02 09:10:00'),
(12,5,'laugh',TIMESTAMP '2025-02-02 09:11:00');

-- Per-post reaction summary.
-- begin-expected
-- columns: title,up_votes,down_votes,emoji_reactions,score
-- row: First Post|2|1|1|1
-- row: Second Post|3|0|1|3
-- row: Third Post|0|1|1|-1
-- end-expected
SELECT
    p.title,
    COUNT(*) FILTER (WHERE r.reaction_type = 'up') AS up_votes,
    COUNT(*) FILTER (WHERE r.reaction_type = 'down') AS down_votes,
    COUNT(*) FILTER (WHERE r.reaction_type IN ('like','love','laugh')) AS emoji_reactions,
    COUNT(*) FILTER (WHERE r.reaction_type = 'up')
      - COUNT(*) FILTER (WHERE r.reaction_type = 'down') AS score
FROM posts p
LEFT JOIN reactions r ON r.post_id = p.id
GROUP BY p.title
ORDER BY p.title;

-- Current-user reacted? (user 2) plus top reaction type.
WITH reaction_counts AS (
    SELECT
        post_id,
        reaction_type,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY COUNT(*) DESC, reaction_type) AS rn
    FROM reactions
    GROUP BY post_id, reaction_type
)
-- begin-expected
-- columns: title,user2_has_reacted,user2_reaction,top_reaction
-- row: First Post|t|up|up
-- row: Second Post|t|up|up
-- row: Third Post|t|down|down
-- end-expected
SELECT
    p.title,
    EXISTS (
        SELECT 1 FROM reactions r
        WHERE r.post_id = p.id AND r.user_id = 2
    ) AS user2_has_reacted,
    (
        SELECT MIN(r.reaction_type)
        FROM reactions r
        WHERE r.post_id = p.id AND r.user_id = 2
    ) AS user2_reaction,
    rc.reaction_type AS top_reaction
FROM posts p
LEFT JOIN reaction_counts rc
  ON rc.post_id = p.id
 AND rc.rn = 1
ORDER BY p.title;

-- Ranking feed by score, then recency.
WITH scored_posts AS (
    SELECT
        p.id,
        p.title,
        p.created_at,
        COUNT(*) FILTER (WHERE r.reaction_type = 'up')
          - COUNT(*) FILTER (WHERE r.reaction_type = 'down') AS score
    FROM posts p
    LEFT JOIN reactions r ON r.post_id = p.id
    GROUP BY p.id, p.title, p.created_at
)
-- begin-expected
-- columns: title,score,rank_in_feed
-- row: Second Post|3|1
-- row: First Post|1|2
-- row: Third Post|-1|3
-- end-expected
SELECT
    title,
    score,
    ROW_NUMBER() OVER (ORDER BY score DESC, created_at DESC, id DESC) AS rank_in_feed
FROM scored_posts
ORDER BY rank_in_feed;

-- Toggle-style upsert flow on a reaction table.
INSERT INTO reactions(post_id, user_id, reaction_type, created_at)
VALUES (12, 1, 'up', TIMESTAMP '2025-02-02 09:30:00')
ON CONFLICT DO NOTHING;

-- begin-expected
-- columns: title,up_votes,down_votes,score
-- row: First Post|2|1|1
-- row: Second Post|3|0|3
-- row: Third Post|1|1|0
-- end-expected
SELECT
    p.title,
    COUNT(*) FILTER (WHERE r.reaction_type = 'up') AS up_votes,
    COUNT(*) FILTER (WHERE r.reaction_type = 'down') AS down_votes,
    COUNT(*) FILTER (WHERE r.reaction_type = 'up')
      - COUNT(*) FILTER (WHERE r.reaction_type = 'down') AS score
FROM posts p
LEFT JOIN reactions r ON r.post_id = p.id
GROUP BY p.title
ORDER BY p.title;
