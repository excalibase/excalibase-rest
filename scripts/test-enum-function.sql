-- Test function with proper enum parameter type
CREATE OR REPLACE FUNCTION get_customers_by_tier_v2(tier_param customer_tier)
RETURNS SETOF customers AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM customers
    WHERE tier = tier_param  -- Both are customer_tier enum, no casting needed
    ORDER BY name;
END;
$$ LANGUAGE plpgsql STABLE;

GRANT EXECUTE ON FUNCTION get_customers_by_tier_v2(customer_tier) TO testuser;

-- Test it
SELECT * FROM get_customers_by_tier_v2('gold'::customer_tier) LIMIT 2;
