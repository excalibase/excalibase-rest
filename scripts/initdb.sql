-- Excalibase REST API Sample Database Schema
-- This script creates sample tables with various PostgreSQL types for demonstration

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create custom enum types
DO $$ BEGIN
    CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE customer_tier AS ENUM ('bronze', 'silver', 'gold', 'platinum');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create custom composite type
DO $$ BEGIN
    CREATE TYPE address AS (
        street VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        zip_code VARCHAR(20),
        country VARCHAR(100)
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Customers table with basic PostgreSQL types (simplified to avoid serialization issues)
CREATE TABLE IF NOT EXISTS customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id SERIAL UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    is_active BOOLEAN DEFAULT true,
    tier customer_tier DEFAULT 'bronze',
    profile JSONB DEFAULT '{}',
    preferred_tags VARCHAR(255), -- Simplified from TEXT[] array
    address_street VARCHAR(255), -- Simplified from address[] array
    address_city VARCHAR(100),
    address_state VARCHAR(50),
    address_zip VARCHAR(20),
    ip_address INET,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Create indexes
    CONSTRAINT customers_email_check CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_tier ON customers(tier);
CREATE INDEX IF NOT EXISTS idx_customers_profile ON customers USING GIN(profile);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10,2) CHECK (cost >= 0),
    weight DECIMAL(8,3),
    dimensions JSONB, -- {width, height, depth, unit}
    categories TEXT[] DEFAULT '{}',
    attributes JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_categories ON products USING GIN(categories);
CREATE INDEX IF NOT EXISTS idx_products_attributes ON products USING GIN(attributes);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    status order_status DEFAULT 'pending',
    subtotal DECIMAL(10,2) NOT NULL CHECK (subtotal >= 0),
    tax_amount DECIMAL(10,2) DEFAULT 0 CHECK (tax_amount >= 0),
    shipping_amount DECIMAL(10,2) DEFAULT 0 CHECK (shipping_amount >= 0),
    discount_amount DECIMAL(10,2) DEFAULT 0 CHECK (discount_amount >= 0),
    total DECIMAL(10,2) GENERATED ALWAYS AS (subtotal + tax_amount + shipping_amount - discount_amount) STORED,
    shipping_address address,
    billing_address address,
    notes TEXT,
    metadata JSONB DEFAULT '{}',
    order_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    shipped_date TIMESTAMPTZ,
    delivered_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_metadata ON orders USING GIN(metadata);

-- Order items table
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    line_total DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    product_snapshot JSONB, -- Store product details at time of order
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- Reviews table
CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(200),
    content TEXT,
    verified_purchase BOOLEAN DEFAULT false,
    helpful_votes INTEGER DEFAULT 0 CHECK (helpful_votes >= 0),
    images TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(product_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_customer_id ON reviews(customer_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);

-- Inventory log table for tracking stock changes
CREATE TABLE IF NOT EXISTS inventory_logs (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    change_type VARCHAR(20) NOT NULL, -- 'sale', 'restock', 'adjustment', 'return'
    quantity_change INTEGER NOT NULL,
    previous_quantity INTEGER NOT NULL,
    new_quantity INTEGER NOT NULL,
    reason TEXT,
    reference_id UUID, -- Could reference orders or other entities
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_inventory_logs_product_id ON inventory_logs(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_logs_created_at ON inventory_logs(created_at);

-- User sessions table (for API usage tracking)
CREATE TABLE IF NOT EXISTS user_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_token VARCHAR(255) UNIQUE NOT NULL,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE CASCADE,
    ip_address INET,
    user_agent TEXT,
    is_active BOOLEAN DEFAULT true,
    last_activity TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_customer_id ON user_sessions(customer_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_token ON user_sessions(session_token);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at);

-- Configuration table for app settings
CREATE TABLE IF NOT EXISTS app_configurations (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    value JSONB NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Sample data insertion
INSERT INTO customers (name, email, phone, date_of_birth, tier, profile, preferred_tags, address_street, address_city, address_state, address_zip, ip_address) VALUES
('John Doe', 'john.doe@example.com', '+1-555-0101', '1990-05-15', 'gold', 
 '{"preferences": {"notifications": true, "theme": "dark"}, "loyalty_points": 1250}',
 'vip,early_adopter', '123 Main St', 'New York', 'NY', '10001', '192.168.1.100'),
 
('Jane Smith', 'jane.smith@example.com', '+1-555-0102', '1985-08-22', 'platinum',
 '{"preferences": {"notifications": false, "theme": "light"}, "loyalty_points": 2500}',
 'premium,frequent_buyer', '456 Oak Ave', 'Los Angeles', 'CA', '90210', '192.168.1.101'),
 
('Bob Johnson', 'bob.johnson@example.com', '+1-555-0103', '1992-12-03', 'silver',
 '{"preferences": {"notifications": true, "theme": "auto"}, "loyalty_points": 750}',
 'new_customer', '789 Pine St', 'Chicago', 'IL', '60601', '10.0.0.50'),
 
('Alice Brown', 'alice.brown@example.com', '+1-555-0104', '1988-03-18', 'bronze',
 '{"preferences": {"notifications": true, "theme": "light"}, "loyalty_points": 100}',
 'occasional_buyer', '321 Elm St', 'Boston', 'MA', '02101', '172.16.0.25'),
 
('Charlie Wilson', 'charlie.wilson@example.com', '+1-555-0105', '1995-11-08', 'gold',
 '{"preferences": {"notifications": false, "theme": "dark"}, "loyalty_points": 1100}',
 'tech_enthusiast,reviewer', '654 Maple Ave', 'Seattle', 'WA', '98101', '203.0.113.45');

INSERT INTO products (sku, name, description, price, cost, weight, dimensions, categories, attributes, stock_quantity) VALUES
('LAPTOP-001', 'Gaming Laptop Pro', 'High-performance gaming laptop with RTX graphics', 1299.99, 899.99, 2.5,
 '{"width": 35.6, "height": 2.3, "depth": 25.1, "unit": "cm"}',
 ARRAY['electronics', 'computers', 'gaming'], 
 '{"brand": "TechBrand", "warranty": "2 years", "color": "black", "specs": {"cpu": "Intel i7", "ram": "16GB", "storage": "512GB SSD"}}',
 25),

('PHONE-001', 'Smartphone X', 'Latest flagship smartphone with advanced camera', 899.99, 599.99, 0.2,
 '{"width": 7.8, "height": 16.2, "depth": 0.8, "unit": "cm"}',
 ARRAY['electronics', 'phones', 'mobile'],
 '{"brand": "PhoneBrand", "warranty": "1 year", "color": "silver", "specs": {"storage": "128GB", "camera": "108MP", "battery": "4000mAh"}}',
 50),

('BOOK-001', 'Programming Guide', 'Complete guide to modern programming', 49.99, 25.99, 0.8,
 '{"width": 18.5, "height": 23.5, "depth": 3.2, "unit": "cm"}',
 ARRAY['books', 'education', 'programming'],
 '{"author": "Tech Author", "pages": 450, "language": "English", "format": "paperback"}',
 100),

('HEADPHONES-001', 'Wireless Headphones', 'Premium noise-cancelling wireless headphones', 299.99, 149.99, 0.3,
 '{"width": 18.0, "height": 20.0, "depth": 8.5, "unit": "cm"}',
 ARRAY['electronics', 'audio', 'accessories'],
 '{"brand": "AudioBrand", "warranty": "2 years", "color": "black", "features": ["noise-cancelling", "wireless", "long-battery"]}',
 75),

('WATCH-001', 'Smart Watch', 'Fitness tracking smartwatch with heart rate monitor', 199.99, 99.99, 0.1,
 '{"width": 4.5, "height": 4.5, "depth": 1.2, "unit": "cm"}',
 ARRAY['electronics', 'wearables', 'fitness'],
 '{"brand": "WatchBrand", "warranty": "1 year", "color": "silver", "features": ["heart-rate", "gps", "waterproof"]}',
 40);

-- Insert sample orders
WITH customer_ids AS (
  SELECT customer_id FROM customers LIMIT 3
)
INSERT INTO orders (order_number, customer_id, status, subtotal, tax_amount, shipping_amount, discount_amount, notes, metadata) 
SELECT 
  'ORD-' || LPAD((ROW_NUMBER() OVER())::text, 6, '0'),
  customer_id,
  (ARRAY['pending', 'processing', 'shipped', 'delivered'])[floor(random() * 4 + 1)]::order_status,
  round((random() * 1000 + 50)::numeric, 2),
  round((random() * 100 + 5)::numeric, 2),
  round((random() * 50 + 10)::numeric, 2),
  round((random() * 100)::numeric, 2),
  'Sample order ' || ROW_NUMBER() OVER(),
  '{"source": "web", "payment_method": "credit_card"}'
FROM customer_ids, generate_series(1, 2);

-- Insert sample order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, product_snapshot)
SELECT 
  o.id,
  p.id,
  floor(random() * 3 + 1)::int,
  p.price,
  jsonb_build_object('name', p.name, 'sku', p.sku, 'price', p.price)
FROM orders o
CROSS JOIN products p
WHERE random() < 0.6; -- Randomly include products in orders

-- Insert sample reviews
INSERT INTO reviews (product_id, customer_id, rating, title, content, verified_purchase, helpful_votes, images)
SELECT 
  p.id,
  c.customer_id,
  floor(random() * 5 + 1)::int,
  'Review for ' || p.name,
  'This is a sample review for ' || p.name || '. ' || 
  CASE 
    WHEN random() > 0.5 THEN 'Great product, highly recommended!'
    ELSE 'Good value for money, satisfied with purchase.'
  END,
  random() > 0.3,
  floor(random() * 10)::int,
  CASE WHEN random() > 0.7 THEN ARRAY['image1.jpg', 'image2.jpg'] ELSE ARRAY[]::text[] END
FROM products p
CROSS JOIN customers c
WHERE random() < 0.4; -- Some customers review some products

-- Insert inventory logs
INSERT INTO inventory_logs (product_id, change_type, quantity_change, previous_quantity, new_quantity, reason)
SELECT 
  id,
  'initial_stock',
  stock_quantity,
  0,
  stock_quantity,
  'Initial inventory setup'
FROM products;

-- Insert app configurations
INSERT INTO app_configurations (key, value, description) VALUES
('site_maintenance', '{"enabled": false, "message": "Site is under maintenance"}', 'Site maintenance settings'),
('payment_methods', '{"enabled": ["credit_card", "paypal", "bank_transfer"], "default": "credit_card"}', 'Available payment methods'),
('shipping_rates', '{"standard": 10.00, "express": 25.00, "overnight": 50.00}', 'Shipping rate configuration'),
('tax_rates', '{"default": 0.08, "states": {"CA": 0.095, "NY": 0.08875, "TX": 0.0825}}', 'Tax rate configuration'),
('feature_flags', '{"new_checkout": true, "recommendations": true, "reviews": true}', 'Feature toggle flags');

-- Create some views for complex queries
CREATE OR REPLACE VIEW customer_order_summary AS
SELECT 
  c.customer_id,
  c.name,
  c.email,
  c.tier,
  COUNT(o.id) as total_orders,
  COALESCE(SUM(o.total), 0) as total_spent,
  COALESCE(AVG(o.total), 0) as avg_order_value,
  MAX(o.order_date) as last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email, c.tier;

CREATE OR REPLACE VIEW product_sales_summary AS
SELECT 
  p.id,
  p.sku,
  p.name,
  p.price,
  COALESCE(SUM(oi.quantity), 0) as total_sold,
  COALESCE(SUM(oi.line_total), 0) as total_revenue,
  COALESCE(AVG(r.rating), 0) as avg_rating,
  COUNT(r.id) as review_count
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
LEFT JOIN reviews r ON p.id = r.product_id
GROUP BY p.id, p.sku, p.name, p.price;

-- Update timestamps trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add update triggers to tables with updated_at columns
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_reviews_updated_at BEFORE UPDATE ON reviews FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_app_configurations_updated_at BEFORE UPDATE ON app_configurations FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_total ON orders(total);
CREATE INDEX IF NOT EXISTS idx_order_items_line_total ON order_items(line_total);
CREATE INDEX IF NOT EXISTS idx_reviews_helpful_votes ON reviews(helpful_votes);

ANALYZE;

-- ====================
-- RLS (ROW LEVEL SECURITY) TEST TABLE
-- ====================
CREATE TABLE rls_orders (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    product TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);

ALTER TABLE rls_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_orders FORCE ROW LEVEL SECURITY;

CREATE POLICY rls_user_isolation ON rls_orders
    FOR ALL USING (user_id = current_setting('request.user_id', true));

INSERT INTO rls_orders (user_id, product, amount) VALUES
    ('alice', 'Widget A', 29.99),
    ('alice', 'Widget B', 49.99),
    ('bob',   'Gadget X', 99.99),
    ('bob',   'Gadget Y', 149.99);

CREATE TABLE rls_payments (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    method TEXT NOT NULL
);

ALTER TABLE rls_payments ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_payments FORCE ROW LEVEL SECURITY;

CREATE POLICY rls_payment_isolation ON rls_payments
    FOR ALL USING (user_id = current_setting('request.user_id', true));

INSERT INTO rls_payments (user_id, amount, method) VALUES
    ('alice', 29.99, 'card'),
    ('alice', 49.99, 'paypal'),
    ('bob',   99.99, 'card');

-- NOTE: Grants to appuser are in 02-roles.sql (runs after this script)