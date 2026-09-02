-- source: investigation.md
-- finding: 54
-- title: `CREATE POLICY` clause/command compatibility is unchecked
-- begin-expected-error
-- sqlstate: 42601
-- message-like: only WITH CHECK expression allowed for INSERT
-- end-expected-error
CREATE POLICY p ON t FOR INSERT USING (id > 0);
--   PG: 42601 only WITH CHECK expression allowed for INSERT | mg: accepted
-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH CHECK cannot be applied to SELECT or DELETE
-- end-expected-error
CREATE POLICY p ON t FOR SELECT WITH CHECK (id > 0);
--   PG: 42601 WITH CHECK cannot be applied to SELECT or DELETE | mg: accepted;
