-- 430_app_localization_and_fallbacks.sql
-- Localization tables, preferred-locale lookup, and fallback queries.

DROP SCHEMA IF EXISTS test_430 CASCADE;
CREATE SCHEMA test_430;
SET search_path TO test_430;

CREATE TABLE users (
    id integer PRIMARY KEY,
    username text NOT NULL UNIQUE,
    preferred_locale text NOT NULL
);

CREATE TABLE articles (
    id integer PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    base_title text NOT NULL
);

CREATE TABLE article_translations (
    article_id integer NOT NULL REFERENCES articles(id),
    locale text NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    PRIMARY KEY (article_id, locale)
);

INSERT INTO users(id, username, preferred_locale) VALUES
(1, 'alice', 'nl'),
(2, 'bob',   'fr'),
(3, 'carol', 'en');

INSERT INTO articles(id, slug, base_title) VALUES
(10, 'welcome', 'Welcome'),
(11, 'pricing', 'Pricing'),
(12, 'about',   'About');

INSERT INTO article_translations(article_id, locale, title, body) VALUES
(10, 'en', 'Welcome', 'English welcome'),
(10, 'nl', 'Welkom', 'Dutch welcome'),
(11, 'en', 'Pricing', 'English pricing'),
(11, 'fr', 'Tarifs', 'French pricing'),
(12, 'en', 'About', 'English about');

-- Effective title with fallback: preferred locale, then English, then base title.
WITH ranked AS (
    SELECT
        u.username,
        a.slug,
        COALESCE(pref.title, en.title, a.base_title) AS effective_title
    FROM users u
    CROSS JOIN articles a
    LEFT JOIN article_translations pref
      ON pref.article_id = a.id
     AND pref.locale = u.preferred_locale
    LEFT JOIN article_translations en
      ON en.article_id = a.id
     AND en.locale = 'en'
)
-- begin-expected
-- columns: username,slug,effective_title
-- row: alice|about|About
-- row: alice|pricing|Pricing
-- row: alice|welcome|Welkom
-- row: bob|about|About
-- row: bob|pricing|Tarifs
-- row: bob|welcome|Welcome
-- row: carol|about|About
-- row: carol|pricing|Pricing
-- row: carol|welcome|Welcome
-- end-expected
SELECT username, slug, effective_title
FROM ranked
ORDER BY username, slug;

-- Missing translation report for non-English locales per article.
-- begin-expected
-- columns: slug,missing_nl,missing_fr
-- row: about|t|t
-- row: pricing|t|f
-- row: welcome|f|t
-- end-expected
SELECT
    a.slug,
    NOT EXISTS (
        SELECT 1 FROM article_translations t
        WHERE t.article_id = a.id AND t.locale = 'nl'
    ) AS missing_nl,
    NOT EXISTS (
        SELECT 1 FROM article_translations t
        WHERE t.article_id = a.id AND t.locale = 'fr'
    ) AS missing_fr
FROM articles a
ORDER BY a.slug;

-- Preferred locale first, fallback locale second, explicitly ranked.
WITH translation_choices AS (
    SELECT
        u.username,
        a.slug,
        t.locale,
        t.title,
        ROW_NUMBER() OVER (
            PARTITION BY u.username, a.slug
            ORDER BY
                CASE
                    WHEN t.locale = u.preferred_locale THEN 1
                    WHEN t.locale = 'en' THEN 2
                    ELSE 3
                END,
                t.locale
        ) AS rn
    FROM users u
    CROSS JOIN articles a
    JOIN article_translations t
      ON t.article_id = a.id
)
-- begin-expected
-- columns: username,slug,locale,title
-- row: alice|about|en|About
-- row: alice|pricing|en|Pricing
-- row: alice|welcome|nl|Welkom
-- row: bob|about|en|About
-- row: bob|pricing|fr|Tarifs
-- row: bob|welcome|en|Welcome
-- row: carol|about|en|About
-- row: carol|pricing|en|Pricing
-- row: carol|welcome|en|Welcome
-- end-expected
SELECT username, slug, locale, title
FROM translation_choices
WHERE rn = 1
ORDER BY username, slug;
