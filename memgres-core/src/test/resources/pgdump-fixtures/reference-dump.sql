--
-- PostgreSQL database dump
--

-- Dumped from database version 18.0
-- Dumped by pg_dump version 18.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_table_access_method = heap;

--
-- Name: order_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.order_status AS ENUM (
    'pending',
    'confirmed',
    'shipped',
    'delivered',
    'cancelled'
);

--
-- Name: customers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.customers (
    id integer NOT NULL,
    name text NOT NULL,
    email character varying(255) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb,
    tags text[],
    active boolean DEFAULT true,
    score numeric(5,2),
    uid uuid
);

--
-- Name: customers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.customers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: customers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.customers_id_seq OWNED BY public.customers.id;

--
-- Name: orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.orders (
    id bigint NOT NULL,
    customer_id integer NOT NULL,
    status public.order_status DEFAULT 'pending'::public.order_status NOT NULL,
    total numeric(10,2) NOT NULL,
    notes text,
    placed_at timestamp with time zone DEFAULT now() NOT NULL,
    shipped_at timestamp with time zone
);

--
-- Name: orders_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.orders_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.orders_id_seq OWNED BY public.orders.id;

--
-- Name: order_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.order_items (
    id integer NOT NULL,
    order_id bigint NOT NULL,
    product_name text NOT NULL,
    quantity integer DEFAULT 1 NOT NULL,
    unit_price numeric(10,2) NOT NULL
);

--
-- Name: order_items_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.order_items_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: order_items_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.order_items_id_seq OWNED BY public.order_items.id;

--
-- Name: customer_summary; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.customer_summary AS
 SELECT c.id,
    c.name,
    c.email,
    count(o.id) AS order_count,
    COALESCE(sum(o.total), (0)::numeric) AS total_spent
   FROM (public.customers c
     LEFT JOIN public.orders o ON ((o.customer_id = c.id)))
  GROUP BY c.id, c.name, c.email;

--
-- Name: customers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customers ALTER COLUMN id SET DEFAULT nextval('public.customers_id_seq'::regclass);

--
-- Name: orders id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.orders ALTER COLUMN id SET DEFAULT nextval('public.orders_id_seq'::regclass);

--
-- Name: order_items id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.order_items ALTER COLUMN id SET DEFAULT nextval('public.order_items_id_seq'::regclass);

--
-- Data for Name: customers; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.customers (id, name, email, created_at, metadata, tags, active, score, uid) FROM stdin;
1	Alice Johnson	alice@example.com	2024-01-15 10:30:00+00	{"tier": "gold", "since": 2020}	{loyal,vip}	t	98.50	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
2	Bob Smith	bob@example.com	2024-02-20 14:00:00+00	{"tier": "silver"}	{new}	t	75.25	b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22
3	Carol White	carol@example.com	2024-03-10 09:15:00+00	{}	\N	f	\N	\N
4	Dave "The Dev" O'Brien	dave@example.com	2024-04-01 00:00:00+00	{"notes": "has\ttabs\nand newlines"}	{dev,tester}	t	100.00	d3eebc99-9c0b-4ef8-bb6d-6bb9bd380a44
5		empty@example.com	2024-05-15 12:00:00+00	\N	{}	t	0.00	e4eebc99-9c0b-4ef8-bb6d-6bb9bd380a55
\.

--
-- Data for Name: orders; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.orders (id, customer_id, status, total, notes, placed_at, shipped_at) FROM stdin;
1	1	confirmed	150.00	Rush order	2024-06-01 10:00:00+00	\N
2	1	shipped	75.50	\N	2024-06-15 11:00:00+00	2024-06-17 09:00:00+00
3	2	pending	200.00	Gift wrap please	2024-07-01 14:30:00+00	\N
4	2	delivered	50.25	\N	2024-05-20 08:00:00+00	2024-05-22 16:00:00+00
5	4	cancelled	999.99	Cancelled by customer	2024-08-01 00:00:00+00	\N
\.

--
-- Data for Name: order_items; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.order_items (id, order_id, product_name, quantity, unit_price) FROM stdin;
1	1	Widget Pro	2	50.00
2	1	Gadget X	1	50.00
3	2	Widget Pro	1	50.00
4	2	Cable Kit	5	5.10
5	3	Mega Bundle	1	200.00
6	4	Widget Mini	5	10.05
7	5	Enterprise License	1	999.99
\.

--
-- Name: customers_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.customers_id_seq', 5, true);

--
-- Name: orders_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.orders_id_seq', 5, true);

--
-- Name: order_items_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.order_items_id_seq', 7, true);

--
-- Name: customers customers_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT customers_email_key UNIQUE (email);

--
-- Name: customers customers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (id);

--
-- Name: order_items order_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.order_items
    ADD CONSTRAINT order_items_pkey PRIMARY KEY (id);

--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);

--
-- Name: idx_customers_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_customers_email ON public.customers USING btree (email);

--
-- Name: idx_orders_customer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orders_customer_id ON public.orders USING btree (customer_id);

--
-- Name: idx_orders_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orders_status ON public.orders USING btree (status);

--
-- Name: order_items order_items_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.order_items
    ADD CONSTRAINT order_items_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.orders(id) ON DELETE CASCADE;

--
-- Name: orders orders_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE RESTRICT;

--
-- Name: TABLE customers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.customers IS 'Core customer records';

--
-- Name: COLUMN customers.metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.customers.metadata IS 'Arbitrary JSON metadata for extensibility';

--
-- Name: COLUMN customers.tags; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.customers.tags IS 'Freeform tags for categorization';

--
-- Name: TABLE orders; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.orders IS 'Customer orders with status tracking';

--
-- Name: INDEX idx_customers_email; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON INDEX public.idx_customers_email IS 'Fast email lookups';

--
-- Name: customers; Type: TABLE; Schema: public; Owner: memgres
--

ALTER TABLE public.customers OWNER TO memgres;
ALTER TABLE public.orders OWNER TO memgres;
ALTER TABLE public.order_items OWNER TO memgres;

--
-- PostgreSQL database dump complete
--
