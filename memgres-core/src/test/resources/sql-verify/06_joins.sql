-- ============================================================
-- 06: JOINs and Advanced Queries
-- ============================================================

-- Setup
CREATE TABLE authors (id serial PRIMARY KEY, name text NOT NULL, country text);
CREATE TABLE books (id serial PRIMARY KEY, title text NOT NULL, author_id int REFERENCES authors(id), published date, price numeric(8,2));
CREATE TABLE reviews (id serial PRIMARY KEY, book_id int REFERENCES books(id), rating int CHECK (rating BETWEEN 1 AND 5), comment text);

INSERT INTO authors (name, country) VALUES ('Author A', 'US'), ('Author B', 'UK'), ('Author C', 'FR'), ('Author D', NULL);
INSERT INTO books (title, author_id, published, price) VALUES
  ('Book 1', 1, '2020-03-15', 19.99), ('Book 2', 1, '2021-07-01', 24.99),
  ('Book 3', 2, '2019-11-20', 14.99), ('Book 4', 2, '2022-01-10', 29.99),
  ('Book 5', 3, '2023-05-05', 9.99), ('Book 6', NULL, '2020-06-01', 12.99);
INSERT INTO reviews (book_id, rating, comment) VALUES
  (1, 5, 'Excellent'), (1, 4, 'Good read'), (2, 3, 'Average'),
  (3, 5, 'Masterpiece'), (3, 4, NULL), (4, 2, 'Disappointing'),
  (5, 5, 'Short but great');

-- === INNER JOIN ===
SELECT b.title, a.name FROM books b INNER JOIN authors a ON a.id = b.author_id ORDER BY b.title;
SELECT b.title, a.name FROM books b JOIN authors a ON a.id = b.author_id ORDER BY b.title;
SELECT b.title, r.rating FROM books b JOIN reviews r ON r.book_id = b.id ORDER BY b.title, r.rating;

-- === LEFT JOIN ===
SELECT b.title, a.name FROM books b LEFT JOIN authors a ON a.id = b.author_id ORDER BY b.title;
SELECT a.name, b.title FROM authors a LEFT JOIN books b ON b.author_id = a.id ORDER BY a.name, b.title;
SELECT a.name, COUNT(b.id) AS book_count FROM authors a LEFT JOIN books b ON b.author_id = a.id GROUP BY a.name ORDER BY a.name;

-- === RIGHT JOIN ===
SELECT b.title, a.name FROM books b RIGHT JOIN authors a ON a.id = b.author_id ORDER BY a.name, b.title;

-- === FULL OUTER JOIN ===
SELECT a.name, b.title FROM authors a FULL OUTER JOIN books b ON a.id = b.author_id ORDER BY a.name, b.title;

-- === CROSS JOIN ===
SELECT a.name, b.title FROM authors a CROSS JOIN books b WHERE a.id = 1 ORDER BY b.title;
SELECT COUNT(*) FROM authors CROSS JOIN books;

-- === Multi-table JOIN ===
SELECT a.name, b.title, r.rating
FROM authors a
JOIN books b ON b.author_id = a.id
JOIN reviews r ON r.book_id = b.id
ORDER BY a.name, b.title, r.rating;

SELECT a.name, b.title, r.rating, r.comment
FROM authors a
JOIN books b ON b.author_id = a.id
LEFT JOIN reviews r ON r.book_id = b.id
ORDER BY a.name, b.title;

-- === Self-join ===
SELECT b1.title, b2.title FROM books b1 JOIN books b2 ON b1.author_id = b2.author_id AND b1.id < b2.id ORDER BY b1.title;

-- === JOIN with USING ===
CREATE TABLE t_join1 (id int PRIMARY KEY, val text);
CREATE TABLE t_join2 (id int PRIMARY KEY, info text);
INSERT INTO t_join1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_join2 VALUES (2, 'x'), (3, 'y'), (4, 'z');
SELECT * FROM t_join1 JOIN t_join2 USING (id) ORDER BY id;
SELECT * FROM t_join1 LEFT JOIN t_join2 USING (id) ORDER BY id;
DROP TABLE t_join1;
DROP TABLE t_join2;

-- === NATURAL JOIN ===
CREATE TABLE nat1 (id int, name text);
CREATE TABLE nat2 (id int, city text);
INSERT INTO nat1 VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO nat2 VALUES (1, 'NYC'), (3, 'London');
SELECT * FROM nat1 NATURAL JOIN nat2;
SELECT * FROM nat1 NATURAL LEFT JOIN nat2 ORDER BY id;
DROP TABLE nat1;
DROP TABLE nat2;

-- === Parenthesized JOINs (pg_dump style) ===
SELECT a.name, b.title, r.rating
FROM ((authors a JOIN books b ON ((b.author_id = a.id))) JOIN reviews r ON ((r.book_id = b.id)))
ORDER BY a.name, b.title;

-- === LATERAL JOIN ===
SELECT a.name, top_book.title, top_book.price
FROM authors a
LEFT JOIN LATERAL (
  SELECT title, price FROM books WHERE author_id = a.id ORDER BY price DESC LIMIT 1
) top_book ON true
ORDER BY a.name;

-- === DISTINCT ON ===
SELECT DISTINCT ON (author_id) author_id, title, price FROM books WHERE author_id IS NOT NULL ORDER BY author_id, price DESC;

-- === Window functions ===
SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM (SELECT a.name, b.price AS salary FROM authors a JOIN books b ON b.author_id = a.id) sub ORDER BY rn;
SELECT title, price, RANK() OVER (ORDER BY price DESC) AS rnk FROM books ORDER BY rnk;
SELECT title, price, SUM(price) OVER (ORDER BY published) AS running_total FROM books WHERE price IS NOT NULL ORDER BY published;
SELECT title, author_id, price, AVG(price) OVER (PARTITION BY author_id) AS avg_by_author FROM books WHERE author_id IS NOT NULL ORDER BY author_id, title;

-- === VALUES clause ===
SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(num, word) ORDER BY num;
SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, label) JOIN authors a ON a.id = t.id;

-- === INVALID: JOIN errors ===
-- Ambiguous column
SELECT id FROM authors JOIN books ON books.author_id = authors.id;
-- Missing ON clause for non-CROSS join
SELECT * FROM authors JOIN books;
-- Reference to unknown table in ON
SELECT * FROM authors a JOIN books b ON b.author_id = c.id;

-- Cleanup
DROP TABLE reviews;
DROP TABLE books;
DROP TABLE authors;
