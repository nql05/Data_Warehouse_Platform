CREATE DATABASE Ecommerce;

CREATE TABLE customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix INTEGER, -- foreign key
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    -- Auto-sets when row is inserted
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Auto-sets when row is inserted, updated via Trigger later
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Geolocation Table
CREATE TABLE geolocation (
    geolocation_zip_code_prefix INTEGER PRIMARY KEY,
    geolocation_lat NUMERIC(18,15),
    geolocation_lng NUMERIC(18,15),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(2),
    -- Auto-sets when row is inserted
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Auto-sets when row is inserted, updated via Trigger later
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE customers ADD FOREIGN KEY (customer_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix);

-- 3. Products Table (Must be created before Order Items)
CREATE TABLE products (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    -- Auto-sets when row is inserted
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Auto-sets when row is inserted, updated via Trigger later
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Sellers Table (Must be created before Order Items)
CREATE TABLE sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix INTEGER REFERENCES geolocation(geolocation_zip_code_prefix),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
);

-- 5. Orders Table (The Centerpiece)
CREATE TABLE orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) REFERENCES customers(customer_id),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    -- Auto-sets when row is inserted
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Auto-sets when row is inserted, updated via Trigger later
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 6. Order Items Table (Links Orders, Products, and Sellers)
CREATE TABLE order_items (
    order_id VARCHAR(32) REFERENCES orders(order_id),
    order_item_id INTEGER,
    product_id VARCHAR(32) REFERENCES products(product_id),
    seller_id VARCHAR(32) REFERENCES sellers(seller_id),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10, 2),
    freight_value NUMERIC(10, 2),
    -- Auto-sets when row is inserted
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Auto-sets when row is inserted, updated via Trigger later
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id)
);

-- 7. Order Payments Table
CREATE TABLE order_payments (
    order_id VARCHAR(32) PRIMARY KEY REFERENCES orders(order_id),
    payment_sequential INTEGER,
    payment_type VARCHAR(20),
    payment_installments INTEGER,
    payment_value NUMERIC(10, 2),
    -- Auto-sets when row is inserted
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Auto-sets when row is inserted, updated via Trigger later
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 8. Order Reviews Table
CREATE TABLE order_reviews (
    review_id VARCHAR(32),
    order_id VARCHAR(32) REFERENCES orders(order_id),
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY(order_id, review_id),
    -- Auto-sets when row is inserted
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Auto-sets when row is inserted, updated via Trigger later
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Function update_updated_at_column()
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to set updated_at column
DO $$
DECLARE
    t text;
BEGIN
    -- Loop through all tables in the 'public' schema
    FOR t IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
    LOOP
        -- Execute the CREATE TRIGGER statement dynamically for each table
        EXECUTE format('
            DROP TRIGGER IF EXISTS set_updated_at ON %I;
            CREATE TRIGGER set_updated_at
            BEFORE UPDATE ON %I
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();',
            t, t);
    END LOOP;
END $$;