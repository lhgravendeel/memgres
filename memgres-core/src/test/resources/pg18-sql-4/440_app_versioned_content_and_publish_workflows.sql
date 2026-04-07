-- 440_app_versioned_content_and_publish_workflows.sql
-- Draft/published versioning and publish workflow queries.

DROP SCHEMA IF EXISTS test_440 CASCADE;
CREATE SCHEMA test_440;
SET search_path TO test_440;

CREATE TABLE documents (
    id integer PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    current_revision_id integer NULL,
    current_published_revision_id integer NULL
);

CREATE TABLE document_revisions (
    id integer PRIMARY KEY,
    document_id integer NOT NULL REFERENCES documents(id),
    version_no integer NOT NULL,
    state text NOT NULL CHECK (state IN ('draft','scheduled','published','archived')),
    title text NOT NULL,
    created_at timestamp NOT NULL,
    published_at timestamp NULL,
    scheduled_for timestamp NULL,
    UNIQUE (document_id, version_no)
);

INSERT INTO documents(id, slug, current_revision_id, current_published_revision_id) VALUES
(1, 'guide',   12, 11),
(2, 'pricing', 21, 21),
(3, 'faq',     31, NULL);

INSERT INTO document_revisions(id, document_id, version_no, state, title, created_at, published_at, scheduled_for) VALUES
(11, 1, 1, 'published', 'Guide v1', TIMESTAMP '2025-01-01 10:00:00', TIMESTAMP '2025-01-02 09:00:00', NULL),
(12, 1, 2, 'draft',     'Guide v2 draft', TIMESTAMP '2025-01-03 10:00:00', NULL, NULL),
(21, 2, 1, 'published', 'Pricing v1', TIMESTAMP '2025-01-01 11:00:00', TIMESTAMP '2025-01-01 12:00:00', NULL),
(22, 2, 2, 'scheduled', 'Pricing v2 scheduled', TIMESTAMP '2025-01-04 09:00:00', NULL, TIMESTAMP '2025-01-10 08:00:00'),
(31, 3, 1, 'draft',     'FAQ v1 draft', TIMESTAMP '2025-01-05 09:30:00', NULL, NULL);

-- Current revision and current published revision side by side.
-- begin-expected
-- columns: slug,current_title,published_title
-- row: faq|FAQ v1 draft|
-- row: guide|Guide v2 draft|Guide v1
-- row: pricing|Pricing v1|Pricing v1
-- end-expected
SELECT
    d.slug,
    cr.title AS current_title,
    pr.title AS published_title
FROM documents d
LEFT JOIN document_revisions cr ON cr.id = d.current_revision_id
LEFT JOIN document_revisions pr ON pr.id = d.current_published_revision_id
ORDER BY d.slug;

-- Effective visible revision at a given time.
WITH effective AS (
    SELECT DISTINCT ON (d.id)
        d.slug,
        r.version_no,
        r.title,
        r.state,
        COALESCE(r.published_at, r.scheduled_for) AS effective_at
    FROM documents d
    JOIN document_revisions r
      ON r.document_id = d.id
    WHERE (
        r.state = 'published' AND r.published_at <= TIMESTAMP '2025-01-09 00:00:00'
    ) OR (
        r.state = 'scheduled' AND r.scheduled_for <= TIMESTAMP '2025-01-09 00:00:00'
    )
    ORDER BY d.id, COALESCE(r.published_at, r.scheduled_for) DESC, r.version_no DESC
)
-- begin-expected
-- columns: slug,version_no,title,state
-- row: guide|1|Guide v1|published
-- row: pricing|1|Pricing v1|published
-- end-expected
SELECT slug, version_no, title, state
FROM effective
ORDER BY slug;

-- Latest revision per document, regardless of state.
WITH latest AS (
    SELECT
        d.slug,
        r.version_no,
        r.state,
        r.title,
        ROW_NUMBER() OVER (
            PARTITION BY d.id
            ORDER BY r.version_no DESC, r.id DESC
        ) AS rn
    FROM documents d
    JOIN document_revisions r ON r.document_id = d.id
)
-- begin-expected
-- columns: slug,version_no,state,title
-- row: faq|1|draft|FAQ v1 draft
-- row: guide|2|draft|Guide v2 draft
-- row: pricing|2|scheduled|Pricing v2 scheduled
-- end-expected
SELECT slug, version_no, state, title
FROM latest
WHERE rn = 1
ORDER BY slug;

-- "Publish scheduled revisions now" workflow.
UPDATE document_revisions
SET state = 'published',
    published_at = scheduled_for,
    scheduled_for = NULL
WHERE state = 'scheduled'
  AND scheduled_for <= TIMESTAMP '2025-01-10 08:00:00';

UPDATE documents d
SET current_published_revision_id = r.id,
    current_revision_id = CASE WHEN d.current_revision_id = d.current_published_revision_id THEN r.id ELSE d.current_revision_id END
FROM document_revisions r
WHERE r.document_id = d.id
  AND r.state = 'published'
  AND r.published_at = (
      SELECT MAX(r2.published_at)
      FROM document_revisions r2
      WHERE r2.document_id = d.id
        AND r2.state = 'published'
  );

-- begin-expected
-- columns: slug,published_title
-- row: faq|
-- row: guide|Guide v1
-- row: pricing|Pricing v2 scheduled
-- end-expected
SELECT d.slug, pr.title AS published_title
FROM documents d
LEFT JOIN document_revisions pr ON pr.id = d.current_published_revision_id
ORDER BY d.slug;
