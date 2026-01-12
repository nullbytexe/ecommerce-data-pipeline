CREATE TABLE IF NOT EXISTS raw.orders (
    order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id VARCHAR(50) NOT NULL,
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20),
    payment_method VARCHAR(20),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID REFERENCES raw.orders(order_id),
    product_id VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(10,2 ) DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    registration_date TIMESTAMP DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Processed data tables
CREATE TABLE IF NOT EXISTS processed.daily_sales (
    date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INTEGER,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed.product_performance (
    product_id VARCHAR(50),
    date DATE,
    total_quantity_sold INTEGER,
    total_revenue DECIMAL(12,2),
    order_count INTEGER,
    PRIMARY KEY (product_id, date)
);

CREATE TABLE IF NOT EXISTS processed.customer_metrics (
    customer_id VARCHAR(50) PRIMARY KEY,
    total_orders INTEGER,
    total_spent DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    first_order_date TIMESTAMP,
    last_order_date TIMESTAMP,
    customer_lifetime_days INTEGER,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Analytics tables
CREATE TABLE IF NOT EXISTS analytics.hourly_metrics (
    metric_hour TIMESTAMP PRIMARY KEY,
    orders_count INTEGER,
    revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    top_product_id VARCHAR(50),
    top_category VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS analytics.real_time_dashboard (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value DECIMAL(12,2),
    timestamp TIMESTAMP DEFAULT NOW()
);

-- Create views
CREATE OR REPLACE VIEW analytics.sales_overview AS 
SELECT
    DATE(o.order_date) as sale_date,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(oi.quantity * oi.unit_price - COALESCE(oi.discount, 0)) as total_revenue,
    AVG(o.total_amount) as avg_order_value, --về bản chất thì không đúng nhưng kết quả thì chính xác
    SUM(oi.quantity) as total_items_sold
FROM raw.orders o
LEFT JOIN raw.order_items oi ON o.order_id = oi.order_id
GROUP BY DATE(o.order_date)
ORDER BY sale_date DESC;

CREATE OR REPLACE VIEW analytics.top_products AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(oi.item_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) as total_revenue
FROM raw.products p 
LEFT JOIN raw.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW analytics.customer_segments AS
SELECT
    customer_id,
    CASE
        WHEN total_spent >= 1000 THEN 'VIP'
        WHEN total_spent >= 500 THEN 'Premium'
        WHEN total_spent >= 100 THEN 'Regular'
        ELSE 'New'
    END as segment,
    total_orders,
    total_spent,
    avg_order_value
FROM processed.customer_metrics;