-- source: review-2026-08.md
-- finding: Root cause 3: DROP ROLE parses a name list and keeps one name
-- area: The parsers
-- title: Root cause 3: DROP ROLE parses a name list and keeps one name
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf2_ra;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf2_rb;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_vf2_rc;
-- begin-expected
-- ok: 0
-- end-expected
DROP ROLE zz_vf2_ra, zz_vf2_rb, zz_vf2_rc;
-- begin-expected
-- columns: rolname:name
-- rowcount: 0
-- end-expected
SELECT rolname FROM pg_roles WHERE rolname IN ('zz_vf2_ra','zz_vf2_rb','zz_vf2_rc') ORDER BY 1;
