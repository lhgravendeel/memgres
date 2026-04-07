-- =============================================================================
-- Complex Query Scenarios — 25 diverse queries exercising subqueries, joins,
-- CTEs, window functions, triggers, generated columns, CASE expressions,
-- JSON aggregation, correlated subqueries, HAVING with expressions, etc.
--
-- Each query block is tagged with --@Q<N> so the Java test can locate it.
-- =============================================================================

-- =============================================================================
-- SCHEMA SETUP
-- =============================================================================

CREATE SCHEMA cqs;
SET search_path = cqs;

-- departments
CREATE TABLE departments (
    id       SERIAL PRIMARY KEY,
    name     TEXT NOT NULL UNIQUE,
    budget   NUMERIC(12,2) NOT NULL DEFAULT 0,
    parent_id INT REFERENCES departments(id)
);

-- employees
CREATE TABLE employees (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    email         TEXT UNIQUE,
    department_id INT REFERENCES departments(id),
    salary        NUMERIC(10,2) NOT NULL,
    hire_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    is_active     BOOLEAN NOT NULL DEFAULT true
);

-- projects
CREATE TABLE projects (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    dept_id     INT REFERENCES departments(id),
    start_date  DATE,
    end_date    DATE,
    status      TEXT CHECK (status IN ('planning','active','completed','cancelled'))
);

-- employee_projects (many-to-many)
CREATE TABLE employee_projects (
    employee_id INT REFERENCES employees(id) ON DELETE CASCADE,
    project_id  INT REFERENCES projects(id) ON DELETE CASCADE,
    role        TEXT NOT NULL DEFAULT 'member',
    hours       INT DEFAULT 0,
    PRIMARY KEY (employee_id, project_id)
);

-- audit_log (trigger target)
CREATE TABLE audit_log (
    id         SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    action     TEXT NOT NULL,
    row_id     INT,
    old_data   TEXT,
    new_data   TEXT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- orders with generated column
CREATE TABLE orders (
    id         SERIAL PRIMARY KEY,
    customer   TEXT NOT NULL,
    quantity   INT NOT NULL,
    unit_price NUMERIC(8,2) NOT NULL,
    total      NUMERIC(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- products with tags (array + json)
CREATE TABLE products (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    category   TEXT,
    price      NUMERIC(8,2),
    tags       TEXT[],
    metadata   JSONB DEFAULT '{}'::jsonb
);

-- time series data
CREATE TABLE metrics (
    id          SERIAL PRIMARY KEY,
    sensor_id   INT NOT NULL,
    measured_at TIMESTAMP NOT NULL,
    value       DOUBLE PRECISION NOT NULL
);

-- tree structure for recursive CTE
CREATE TABLE categories (
    id        SERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    parent_id INT REFERENCES categories(id)
);

-- =============================================================================
-- TRIGGER FUNCTION: audit employee salary changes
-- =============================================================================
CREATE OR REPLACE FUNCTION cqs.fn_audit_salary()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.salary <> NEW.salary THEN
        INSERT INTO cqs.audit_log(table_name, action, row_id, old_data, new_data)
        VALUES ('employees', 'salary_change', NEW.id,
                OLD.salary::text, NEW.salary::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_salary_audit
    AFTER UPDATE ON employees
    FOR EACH ROW EXECUTE FUNCTION cqs.fn_audit_salary();

-- =============================================================================
-- SEED DATA
-- =============================================================================

-- departments (hierarchical)
INSERT INTO departments (name, budget, parent_id) VALUES
    ('Engineering',  500000, NULL),
    ('Backend',      200000, 1),
    ('Frontend',     150000, 1),
    ('Sales',        300000, NULL),
    ('Enterprise',   180000, 4),
    ('Marketing',    250000, NULL);

-- employees
INSERT INTO employees (name, email, department_id, salary, hire_date, is_active) VALUES
    ('Alice',   'alice@co.com',   2, 120000, '2020-01-15', true),
    ('Bob',     'bob@co.com',     2, 110000, '2021-03-20', true),
    ('Charlie', 'charlie@co.com', 3, 105000, '2019-06-01', true),
    ('Diana',   'diana@co.com',   4, 95000,  '2022-01-10', true),
    ('Eve',     'eve@co.com',     5, 130000, '2018-09-05', true),
    ('Frank',   'frank@co.com',   3, 98000,  '2023-07-15', true),
    ('Grace',   'grace@co.com',   6, 88000,  '2021-11-01', true),
    ('Hank',    'hank@co.com',    2, 115000, '2020-04-22', true),
    ('Ivy',     'ivy@co.com',     6, 92000,  '2022-08-30', false),
    ('Jack',    'jack@co.com',    1, 150000, '2017-02-14', true);

-- projects
INSERT INTO projects (name, dept_id, start_date, end_date, status) VALUES
    ('API v2',       2, '2024-01-01', '2024-06-30', 'active'),
    ('Dashboard',    3, '2024-02-01', '2024-09-30', 'active'),
    ('Sales Portal', 4, '2024-03-01', NULL,         'planning'),
    ('Rebrand',      6, '2023-06-01', '2023-12-31', 'completed'),
    ('Data Lake',    1, '2024-04-01', '2025-03-31', 'active');

-- employee_projects
INSERT INTO employee_projects (employee_id, project_id, role, hours) VALUES
    (1, 1, 'lead',   320),
    (2, 1, 'member', 280),
    (8, 1, 'member', 200),
    (3, 2, 'lead',   350),
    (6, 2, 'member', 150),
    (4, 3, 'lead',   100),
    (5, 3, 'member', 80),
    (7, 4, 'lead',   400),
    (9, 4, 'member', 300),
    (10,5, 'lead',   250),
    (1, 5, 'member', 120),
    (2, 5, 'member', 90);

-- orders
INSERT INTO orders (customer, quantity, unit_price) VALUES
    ('Acme Corp',   10, 49.99),
    ('Globex',       5, 199.00),
    ('Initech',      3, 75.50),
    ('Acme Corp',   20, 49.99),
    ('Globex',       1, 999.00),
    ('Umbrella',     8, 120.00);

-- products
INSERT INTO products (name, category, price, tags, metadata) VALUES
    ('Widget A', 'hardware', 29.99, ARRAY['small','blue'],   '{"weight": 0.5, "origin": "US"}'),
    ('Widget B', 'hardware', 49.99, ARRAY['medium','red'],   '{"weight": 1.2, "origin": "DE"}'),
    ('Gizmo',   'software', 99.99, ARRAY['enterprise'],      '{"license": "annual", "seats": 10}'),
    ('Gadget',  'hardware', 149.99,ARRAY['large','premium'], '{"weight": 3.5, "origin": "JP"}'),
    ('Plugin',  'software', 19.99, ARRAY['small','free'],    '{"license": "perpetual"}');

-- metrics (time series)
INSERT INTO metrics (sensor_id, measured_at, value) VALUES
    (1, '2024-01-01 00:00:00', 23.5),
    (1, '2024-01-01 01:00:00', 24.1),
    (1, '2024-01-01 02:00:00', 22.8),
    (2, '2024-01-01 00:00:00', 55.0),
    (2, '2024-01-01 01:00:00', 57.3),
    (2, '2024-01-01 02:00:00', 54.9),
    (1, '2024-01-01 03:00:00', 25.0),
    (2, '2024-01-01 03:00:00', 56.2),
    (1, '2024-01-01 04:00:00', 23.9),
    (2, '2024-01-01 04:00:00', 58.1);

-- categories (tree)
INSERT INTO categories (name, parent_id) VALUES
    ('Electronics', NULL),
    ('Computers', 1),
    ('Laptops', 2),
    ('Desktops', 2),
    ('Phones', 1),
    ('Clothing', NULL),
    ('Mens', 6),
    ('Womens', 6);
