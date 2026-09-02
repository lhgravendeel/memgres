-- source: investigation.md
-- finding: 106
-- title: Foreign key definition validation is entirely absent ⚠️ high (14 cases)
-- unrunnable: the report wrote this reproducer abbreviated
-- referenced column is not unique
FOREIGN KEY (ftest1) REFERENCES pk(ptest2);
--   PG: 42830 there is no unique constraint matching given keys | mg: created

-- column counts disagree
FOREIGN KEY (ftest1, ftest2) REFERENCES pk(ptest1);
--   PG: 42830 number of referencing and referenced columns disagree | mg: created

-- duplicate column in the referenced list
FOREIGN KEY (ftest1, ftest1) REFERENCES pk(ptest1, ptest2);
--   PG: 42830 referenced-columns list must not contain duplicates | mg: created

-- referenced column does not exist
FOREIGN KEY (ftest1) REFERENCES pk(nosuchcol);
--   PG: 42703 column does not exist | mg: created

-- system column used in a key
FOREIGN KEY (tableoid) REFERENCES pk(someoid);
--   PG: 0A000 system columns cannot be used in foreign keys | mg: created

-- referenced table has no primary key at all
ftest1 int REFERENCES nopk;
--   PG: 42704 there is no primary key for referenced table | mg: created

-- types are not comparable: inet referencing an integer
ftest1 inet REFERENCES pk;
--   PG: 42804 foreign key constraint cannot be implemented | mg: created
--   (also cidr/timestamp against int/int, and a swapped-order composite key)
… ON DELETE SET NULL (bar)   -- no such column;  PG: 42703 | mg: created
… ON DELETE SET NULL (foo)   -- not part of the FK; PG: 42P10 | mg: created
… ON UPDATE SET NULL (id)    -- PG: 0A000 column list only supported for ON DELETE | mg: created
FOREIGN KEY (a, b) REFERENCES pk MATCH PARTIAL;
--   PG: 0A000 MATCH PARTIAL not yet implemented | mg: created;;
