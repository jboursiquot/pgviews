CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  product_id INT REFERENCES products(id),
  quantity INT NOT NULL,
  order_date DATE NOT NULL
);

CREATE INDEX idx_orders_product_id ON orders(product_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);

-- Create regular view for category sales
CREATE OR REPLACE VIEW category_sales_view AS
SELECT 
  p.category,
  o.order_date,
  SUM(o.quantity) AS daily_total_sold
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category, o.order_date;

-- Create materialized view for category sales
CREATE MATERIALIZED VIEW IF NOT EXISTS category_sales_materialized_view AS
SELECT 
  p.category,
  o.order_date,
  SUM(o.quantity) AS daily_total_sold
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category, o.order_date
WITH DATA;

-- Add indexes to the materialized view for faster filtering
CREATE INDEX IF NOT EXISTS idx_category_sales_materialized_view_category ON category_sales_materialized_view(category);
CREATE INDEX IF NOT EXISTS idx_category_sales_materialized_view_order_date ON category_sales_materialized_view(order_date);

INSERT INTO products (name, category) VALUES
  ('Laptop', 'Electronics'),
  ('Smartphone', 'Electronics'),
  ('Coffee Maker', 'Home Appliances'),
  ('Desk Lamp', 'Home Decor'),
  ('Running Shoes', 'Sportswear')
ON CONFLICT DO NOTHING;

-- Insert sample orders for each category for benchmarks
DO $$
DECLARE
    i INT;
    category_name TEXT;
BEGIN
    FOR category_name IN SELECT DISTINCT category FROM products LOOP
        FOR i IN 1..1000 LOOP
            INSERT INTO orders (product_id, quantity, order_date)
            VALUES (
                (SELECT id FROM products WHERE category = category_name ORDER BY random() LIMIT 1),
                (random() * 5 + 1)::INT,
                CURRENT_DATE - (random() * 60)::INT
            );
        END LOOP;
    END LOOP;
END $$;


-- Refresh the materialized view after loading data
REFRESH MATERIALIZED VIEW category_sales_materialized_view;
