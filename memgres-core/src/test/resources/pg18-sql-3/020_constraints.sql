DROP SCHEMA IF EXISTS test_020 CASCADE;
CREATE SCHEMA test_020;
SET search_path TO test_020;

CREATE TABLE departments (
    dept_id integer PRIMARY KEY,
    dept_code text UNIQUE NOT NULL,
    dept_name text NOT NULL
);

CREATE TABLE employees (
    emp_id integer PRIMARY KEY,
    dept_id integer REFERENCES departments(dept_id) ON UPDATE CASCADE ON DELETE SET NULL,
    email text NOT NULL,
    salary numeric(10,2) NOT NULL CHECK (salary >= 0),
    active boolean NOT NULL DEFAULT true,
    CONSTRAINT uq_employee_email UNIQUE (email)
);

CREATE TABLE project_assignments (
    emp_id integer NOT NULL,
    project_code text NOT NULL,
    role_name text NOT NULL,
    CONSTRAINT pk_assignment PRIMARY KEY (emp_id, project_code),
    CONSTRAINT fk_assignment_employee FOREIGN KEY (emp_id)
        REFERENCES employees(emp_id)
        ON DELETE CASCADE
);

CREATE TABLE parents (
    id integer PRIMARY KEY
);

CREATE TABLE children (
    id integer PRIMARY KEY,
    parent_id integer,
    CONSTRAINT fk_child_parent FOREIGN KEY (parent_id)
        REFERENCES parents(id)
        DEFERRABLE INITIALLY DEFERRED
);

INSERT INTO departments VALUES
    (1, 'ENG', 'Engineering'),
    (2, 'HR', 'Human Resources');

INSERT INTO employees(emp_id, dept_id, email, salary) VALUES
    (1, 1, 'alice@example.com', 1000.00),
    (2, 2, 'bob@example.com', 1200.00);

INSERT INTO project_assignments VALUES
    (1, 'P1', 'owner'),
    (1, 'P2', 'reviewer'),
    (2, 'P2', 'owner');

BEGIN;
INSERT INTO children(id, parent_id) VALUES (1, 100);
INSERT INTO parents(id) VALUES (100);
COMMIT;

-- begin-expected
-- columns: emp_id|dept_id|email|salary|active
-- row: 1|1|alice@example.com|1000.00|true
-- row: 2|2|bob@example.com|1200.00|true
-- end-expected
SELECT emp_id, dept_id, email, salary, active
FROM employees
ORDER BY emp_id;

UPDATE departments
SET dept_id = 20
WHERE dept_id = 2;

DELETE FROM departments
WHERE dept_id = 1;

-- begin-expected
-- columns: emp_id|dept_id|email
-- row: 1||alice@example.com
-- row: 2|20|bob@example.com
-- end-expected
SELECT emp_id, dept_id, email
FROM employees
ORDER BY emp_id;

-- begin-expected
-- columns: id|parent_id
-- row: 1|100
-- end-expected
SELECT id, parent_id
FROM children;

-- begin-expected-error
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO employees(emp_id, dept_id, email, salary) VALUES
    (3, 20, 'alice@example.com', 900.00);

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO employees(emp_id, dept_id, email, salary) VALUES
    (4, 20, 'charlie@example.com', -1.00);

-- begin-expected-error
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO project_assignments(emp_id, project_code, role_name) VALUES
    (999, 'PX', 'ghost');

DROP SCHEMA test_020 CASCADE;
