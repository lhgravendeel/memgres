CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);
