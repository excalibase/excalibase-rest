-- ============================================
-- Test Functions for RPC and Computed Fields
-- ============================================

-- ==================== RPC FUNCTIONS ====================

-- 1. Calculate discount based on customer tier
CREATE OR REPLACE FUNCTION calculate_discount(
    customer_tier text,
    order_amount decimal
)
RETURNS decimal AS $$
BEGIN
    RETURN CASE customer_tier
        WHEN 'platinum' THEN order_amount * 0.20
        WHEN 'gold' THEN order_amount * 0.15
        WHEN 'silver' THEN order_amount * 0.10
        WHEN 'bronze' THEN order_amount * 0.05
        ELSE 0.00
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 2. Get customers by tier (returns table)
CREATE OR REPLACE FUNCTION get_customers_by_tier(tier_name text)
RETURNS SETOF customers AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM customers
    WHERE tier = tier_name
    ORDER BY name;
END;
$$ LANGUAGE plpgsql STABLE;

-- 3. Calculate order statistics for a customer
CREATE OR REPLACE FUNCTION get_customer_order_stats(customer_id_param integer)
RETURNS TABLE(
    total_orders bigint,
    total_spent decimal,
    avg_order_value decimal,
    max_order_value decimal,
    min_order_value decimal
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::bigint as total_orders,
        COALESCE(SUM(total), 0) as total_spent,
        COALESCE(AVG(total), 0) as avg_order_value,
        COALESCE(MAX(total), 0) as max_order_value,
        COALESCE(MIN(total), 0) as min_order_value
    FROM orders
    WHERE orders.customer_id = customer_id_param;
END;
$$ LANGUAGE plpgsql STABLE;

-- 4. Get product sales summary (complex aggregation)
CREATE OR REPLACE FUNCTION get_product_sales(product_id_param integer)
RETURNS TABLE(
    product_name text,
    total_quantity bigint,
    total_revenue decimal,
    avg_unit_price decimal
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.name::text as product_name,
        COALESCE(SUM(oi.quantity), 0)::bigint as total_quantity,
        COALESCE(SUM(oi.line_total), 0) as total_revenue,
        COALESCE(AVG(oi.unit_price), 0) as avg_unit_price
    FROM products p
    LEFT JOIN order_items oi ON p.id = oi.product_id
    WHERE p.id = product_id_param
    GROUP BY p.id, p.name;
END;
$$ LANGUAGE plpgsql STABLE;

-- ==================== COMPUTED FIELDS ====================

-- 1. Customer full address (row function)
CREATE OR REPLACE FUNCTION customers_full_address(customers)
RETURNS text AS $$
    SELECT $1.address_street || ', ' || $1.address_city || ', ' ||
           $1.address_state || ' ' || $1.address_zip;
$$ LANGUAGE SQL IMMUTABLE;

-- 2. Customer loyalty level (computed from points)
CREATE OR REPLACE FUNCTION customers_loyalty_level(customers)
RETURNS text AS $$
    SELECT CASE
        WHEN ($1.profile->>'loyalty_points')::int >= 2000 THEN 'Elite'
        WHEN ($1.profile->>'loyalty_points')::int >= 1000 THEN 'Premium'
        WHEN ($1.profile->>'loyalty_points')::int >= 500 THEN 'Standard'
        ELSE 'Basic'
    END;
$$ LANGUAGE SQL IMMUTABLE;

-- 3. Customer display name (with tier badge)
CREATE OR REPLACE FUNCTION customers_display_name(customers)
RETURNS text AS $$
    SELECT $1.name || ' [' || UPPER($1.tier) || ']';
$$ LANGUAGE SQL IMMUTABLE;

-- 4. Product display price (formatted with currency)
CREATE OR REPLACE FUNCTION products_display_price(products)
RETURNS text AS $$
    SELECT '$' || $1.price::text;
$$ LANGUAGE SQL IMMUTABLE;

-- 5. Product stock status (computed field)
CREATE OR REPLACE FUNCTION products_stock_status(products)
RETURNS text AS $$
    SELECT CASE
        WHEN $1.stock_quantity > 100 THEN 'In Stock'
        WHEN $1.stock_quantity > 10 THEN 'Low Stock'
        WHEN $1.stock_quantity > 0 THEN 'Very Low Stock'
        ELSE 'Out of Stock'
    END;
$$ LANGUAGE SQL IMMUTABLE;

-- 6. Order summary text (row function)
CREATE OR REPLACE FUNCTION orders_summary(orders)
RETURNS text AS $$
    SELECT 'Order ' || $1.order_number || ': $' || $1.total::text ||
           ' (' || $1.status || ')';
$$ LANGUAGE SQL IMMUTABLE;

-- ==================== GRANT PERMISSIONS ====================

GRANT EXECUTE ON FUNCTION calculate_discount(text, decimal) TO testuser;
GRANT EXECUTE ON FUNCTION get_customers_by_tier(text) TO testuser;
GRANT EXECUTE ON FUNCTION get_customer_order_stats(integer) TO testuser;
GRANT EXECUTE ON FUNCTION get_product_sales(integer) TO testuser;

GRANT EXECUTE ON FUNCTION customers_full_address(customers) TO testuser;
GRANT EXECUTE ON FUNCTION customers_loyalty_level(customers) TO testuser;
GRANT EXECUTE ON FUNCTION customers_display_name(customers) TO testuser;
GRANT EXECUTE ON FUNCTION products_display_price(products) TO testuser;
GRANT EXECUTE ON FUNCTION products_stock_status(products) TO testuser;
GRANT EXECUTE ON FUNCTION orders_summary(orders) TO testuser;

-- ==================== VERIFICATION ====================

-- Test RPC functions
SELECT calculate_discount('gold', 100.00);
SELECT * FROM get_customers_by_tier('gold') LIMIT 2;

-- Test computed fields
SELECT
    c.name,
    customers_full_address(c) as full_address,
    customers_loyalty_level(c) as loyalty_level,
    customers_display_name(c) as display_name
FROM customers c
LIMIT 3;
