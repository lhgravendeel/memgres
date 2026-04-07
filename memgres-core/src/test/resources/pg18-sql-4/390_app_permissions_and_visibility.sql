-- 390_app_permissions_and_visibility.sql
-- Application-style permission and visibility resolution queries.
-- Option B expectation format.

DROP SCHEMA IF EXISTS test_390 CASCADE;
CREATE SCHEMA test_390;
SET search_path TO test_390;

CREATE TABLE users (
    id integer PRIMARY KEY,
    username text NOT NULL UNIQUE,
    is_admin boolean NOT NULL DEFAULT false
);

CREATE TABLE organizations (
    id integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE teams (
    id integer PRIMARY KEY,
    org_id integer NOT NULL REFERENCES organizations(id),
    name text NOT NULL
);

CREATE TABLE projects (
    id integer PRIMARY KEY,
    org_id integer NOT NULL REFERENCES organizations(id),
    team_id integer REFERENCES teams(id),
    name text NOT NULL,
    visibility text NOT NULL CHECK (visibility IN ('public','org','private')),
    owner_user_id integer NOT NULL REFERENCES users(id),
    archived_at timestamp NULL
);

CREATE TABLE org_memberships (
    org_id integer NOT NULL REFERENCES organizations(id),
    user_id integer NOT NULL REFERENCES users(id),
    role_name text NOT NULL,
    PRIMARY KEY (org_id, user_id)
);

CREATE TABLE team_memberships (
    team_id integer NOT NULL REFERENCES teams(id),
    user_id integer NOT NULL REFERENCES users(id),
    role_name text NOT NULL,
    PRIMARY KEY (team_id, user_id)
);

CREATE TABLE project_memberships (
    project_id integer NOT NULL REFERENCES projects(id),
    user_id integer NOT NULL REFERENCES users(id),
    role_name text NOT NULL,
    PRIMARY KEY (project_id, user_id)
);

CREATE TABLE project_bans (
    project_id integer NOT NULL REFERENCES projects(id),
    user_id integer NOT NULL REFERENCES users(id),
    reason text NOT NULL,
    PRIMARY KEY (project_id, user_id)
);

INSERT INTO users(id, username, is_admin) VALUES
(1, 'alice', false),
(2, 'bob', false),
(3, 'carol', false),
(4, 'dave', true),
(5, 'erin', false);

INSERT INTO organizations(id, name) VALUES
(10, 'Acme'),
(20, 'Globex');

INSERT INTO teams(id, org_id, name) VALUES
(100, 10, 'Platform'),
(101, 10, 'Docs'),
(200, 20, 'Infra');

INSERT INTO projects(id, org_id, team_id, name, visibility, owner_user_id, archived_at) VALUES
(1000, 10, 100, 'alpha-public',  'public',  1, NULL),
(1001, 10, 100, 'alpha-org',     'org',     1, NULL),
(1002, 10, 101, 'alpha-private', 'private', 2, NULL),
(1003, 20, 200, 'globex-private','private', 3, NULL),
(1004, 10, NULL,'owner-only',    'private', 5, NULL),
(1005, 10, 100, 'archived-org',  'org',     1, TIMESTAMP '2024-01-01 00:00:00');

INSERT INTO org_memberships(org_id, user_id, role_name) VALUES
(10, 1, 'owner'),
(10, 2, 'member'),
(10, 5, 'member'),
(20, 3, 'owner');

INSERT INTO team_memberships(team_id, user_id, role_name) VALUES
(100, 1, 'maintainer'),
(100, 2, 'member'),
(101, 2, 'maintainer'),
(200, 3, 'maintainer');

INSERT INTO project_memberships(project_id, user_id, role_name) VALUES
(1001, 2, 'editor'),
(1002, 1, 'viewer'),
(1002, 2, 'owner'),
(1003, 3, 'owner'),
(1004, 1, 'viewer');

INSERT INTO project_bans(project_id, user_id, reason) VALUES
(1001, 5, 'suspended');

-- Effective project visibility for each user, with common application policy:
-- admin OR owner OR project member OR team member OR org member for org-visible OR public,
-- but bans override all except admin. Archived projects are excluded.
WITH visibility_matrix AS (
    SELECT
        u.username,
        p.name AS project_name,
        CASE
            WHEN u.is_admin THEN true
            WHEN EXISTS (
                SELECT 1
                FROM project_bans pb
                WHERE pb.project_id = p.id AND pb.user_id = u.id
            ) THEN false
            WHEN p.owner_user_id = u.id THEN true
            WHEN EXISTS (
                SELECT 1 FROM project_memberships pm
                WHERE pm.project_id = p.id AND pm.user_id = u.id
            ) THEN true
            WHEN p.team_id IS NOT NULL AND EXISTS (
                SELECT 1 FROM team_memberships tm
                WHERE tm.team_id = p.team_id AND tm.user_id = u.id
            ) THEN true
            WHEN p.visibility = 'public' THEN true
            WHEN p.visibility = 'org' AND EXISTS (
                SELECT 1 FROM org_memberships om
                WHERE om.org_id = p.org_id AND om.user_id = u.id
            ) THEN true
            ELSE false
        END AS can_view
    FROM users u
    CROSS JOIN projects p
    WHERE p.archived_at IS NULL
)
-- begin-expected
-- columns: username,project_name,can_view
-- row: alice|alpha-org|t
-- row: alice|alpha-private|t
-- row: alice|alpha-public|t
-- row: alice|owner-only|t
-- row: bob|alpha-org|t
-- row: bob|alpha-private|t
-- row: bob|alpha-public|t
-- row: carol|alpha-public|t
-- row: carol|globex-private|t
-- row: dave|alpha-org|t
-- row: dave|alpha-private|t
-- row: dave|alpha-public|t
-- row: dave|globex-private|t
-- row: dave|owner-only|t
-- row: erin|alpha-public|t
-- row: erin|owner-only|t
-- end-expected
SELECT username, project_name, can_view
FROM visibility_matrix
WHERE can_view
ORDER BY username, project_name;

-- Direct membership / inherited access classification.
WITH access_paths AS (
    SELECT
        u.username,
        p.name AS project_name,
        CASE
            WHEN u.is_admin THEN 'admin'
            WHEN p.owner_user_id = u.id THEN 'owner'
            WHEN EXISTS (
                SELECT 1 FROM project_memberships pm
                WHERE pm.project_id = p.id AND pm.user_id = u.id
            ) THEN 'project-member'
            WHEN p.team_id IS NOT NULL AND EXISTS (
                SELECT 1 FROM team_memberships tm
                WHERE tm.team_id = p.team_id AND tm.user_id = u.id
            ) THEN 'team-member'
            WHEN p.visibility = 'org' AND EXISTS (
                SELECT 1 FROM org_memberships om
                WHERE om.org_id = p.org_id AND om.user_id = u.id
            ) THEN 'org-member'
            WHEN p.visibility = 'public' THEN 'public'
            ELSE 'none'
        END AS access_path
    FROM users u
    CROSS JOIN projects p
    WHERE p.archived_at IS NULL
)
-- begin-expected
-- columns: username,project_name,access_path
-- row: alice|alpha-org|owner
-- row: alice|alpha-private|project-member
-- row: alice|alpha-public|owner
-- row: alice|globex-private|none
-- row: alice|owner-only|project-member
-- row: bob|alpha-org|project-member
-- row: bob|alpha-private|owner
-- row: bob|alpha-public|team-member
-- row: bob|globex-private|none
-- row: bob|owner-only|none
-- row: carol|alpha-org|none
-- row: carol|alpha-private|none
-- row: carol|alpha-public|public
-- row: carol|globex-private|owner
-- row: carol|owner-only|none
-- row: dave|alpha-org|admin
-- row: dave|alpha-private|admin
-- row: dave|alpha-public|admin
-- row: dave|globex-private|admin
-- row: dave|owner-only|admin
-- row: erin|alpha-org|org-member
-- row: erin|alpha-private|none
-- row: erin|alpha-public|public
-- row: erin|globex-private|none
-- row: erin|owner-only|owner
-- end-expected
SELECT username, project_name, access_path
FROM access_paths
ORDER BY username, project_name;

-- Common "can edit" check.
WITH editable AS (
    SELECT
        u.username,
        p.name AS project_name,
        CASE
            WHEN u.is_admin THEN true
            WHEN p.owner_user_id = u.id THEN true
            WHEN EXISTS (
                SELECT 1 FROM project_memberships pm
                WHERE pm.project_id = p.id
                  AND pm.user_id = u.id
                  AND pm.role_name IN ('owner','editor')
            ) THEN true
            WHEN p.team_id IS NOT NULL AND EXISTS (
                SELECT 1 FROM team_memberships tm
                WHERE tm.team_id = p.team_id
                  AND tm.user_id = u.id
                  AND tm.role_name = 'maintainer'
            ) THEN true
            ELSE false
        END AS can_edit
    FROM users u
    CROSS JOIN projects p
    WHERE p.archived_at IS NULL
)
-- begin-expected
-- columns: username,project_name
-- row: alice|alpha-org
-- row: alice|alpha-public
-- row: bob|alpha-org
-- row: bob|alpha-private
-- row: bob|alpha-public
-- row: carol|globex-private
-- row: dave|alpha-org
-- row: dave|alpha-private
-- row: dave|alpha-public
-- row: dave|globex-private
-- row: dave|owner-only
-- row: erin|owner-only
-- end-expected
SELECT username, project_name
FROM editable
WHERE can_edit
ORDER BY username, project_name;
