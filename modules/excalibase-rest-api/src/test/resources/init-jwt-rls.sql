CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product TEXT NOT NULL,
    total NUMERIC(10,2) NOT NULL
);
INSERT INTO orders (user_id, product, total) VALUES
    (42, 'Widget', 9.99), (42, 'Gadget', 19.99), (99, 'Other', 5.00);

CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    method TEXT NOT NULL
);
INSERT INTO payments (user_id, amount, method) VALUES
    (42, 9.99, 'card'), (42, 19.99, 'card'), (42, 5.00, 'paypal'), (99, 15.00, 'card');

ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;
CREATE POLICY user_orders ON orders FOR ALL USING (
    user_id = NULLIF(current_setting('request.user_id', true), '')::integer
);

ALTER TABLE payments ENABLE ROW LEVEL SECURITY;
ALTER TABLE payments FORCE ROW LEVEL SECURITY;
CREATE POLICY user_payments ON payments FOR ALL USING (
    user_id = NULLIF(current_setting('request.user_id', true), '')::integer
);

CREATE ROLE app_user WITH LOGIN PASSWORD 'apppass';
GRANT ALL ON ALL TABLES IN SCHEMA public TO app_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO app_user;
