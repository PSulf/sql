--COALESCE
SELECT 
  product_name || ', ' || COALESCE(product_size, '') || ' (' || COALESCE(product_qty_type, 'unit') || ')' AS product_full_description
FROM product;

--Windowed Functions
--Q1
SELECT 
  customer_id,
  market_date,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY market_date) AS visit_number
FROM customer_purchases;

SELECT 
  customer_id, 
  market_date,
  DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY market_date) AS visit_number
FROM customer_purchases;

--Q2
WITH visit_ranking AS (
  SELECT 
    customer_id,
    market_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY market_date DESC) AS rn
  FROM customer_purchases
)
SELECT 
  customer_id,
  market_date
FROM visit_ranking
WHERE rn = 1;

SELECT 
  customer_id,
  product_id,
  COUNT(*) OVER (PARTITION BY customer_id, product_id) AS purchase_count
FROM customer_purchases;

--String manipulation
SELECT 
  product_name,
  CASE 
    WHEN INSTR(product_name, '-') > 0 THEN TRIM(SUBSTR(product_name, INSTR(product_name, '-') + 1))
    ELSE NULL
  END AS description
FROM product;

SELECT 
  product_name,
  product_size
FROM product
WHERE product_size REGEXP '[0-9]';

-- UNION
WITH daily_sales AS (
  SELECT 
    market_date,
    SUM(quantity * cost_to_customer_per_qty) AS total_sales
  FROM customer_purchases
  GROUP BY market_date
),
ranked_sales AS (
  SELECT 
    market_date,
    total_sales,
    RANK() OVER (ORDER BY total_sales DESC) AS sales_rank_high,
    RANK() OVER (ORDER BY total_sales ASC) AS sales_rank_low
  FROM daily_sales
)

SELECT market_date, total_sales, 'Highest Sales' AS sales_type
FROM ranked_sales
WHERE sales_rank_high = 1

UNION

SELECT market_date, total_sales, 'Lowest Sales' AS sales_type
FROM ranked_sales
WHERE sales_rank_low = 1

ORDER BY total_sales DESC;



-- Cross Join
WITH product_vendor_info AS (
    SELECT
        v.vendor_name,
        p.product_name,
        p.product_id,
        vi.vendor_id,
        vi.original_price AS product_price
    FROM vendor_inventory vi
    JOIN product p ON vi.product_id = p.product_id
    JOIN vendor v ON vi.vendor_id = v.vendor_id
),
cross_joined_sales AS (
    SELECT
        pvi.vendor_name,
        pvi.product_name,
        pvi.product_price,
        c.customer_id,
        5 AS quantity -- Each customer buys 5 of each product
    FROM product_vendor_info pvi
    CROSS JOIN customer c
)

SELECT
    vendor_name,
    product_name,
    SUM(quantity * product_price) AS potential_revenue
FROM cross_joined_sales
GROUP BY vendor_name, product_name
ORDER BY vendor_name, product_name;


-- First drop table if it exists to avoid errors
DROP TABLE IF EXISTS product_units;

-- Then create the table
CREATE TABLE product_units AS
SELECT 
    product_id,
    product_name, 
    product_size,
    product_category_id,
    product_qty_type,
    CURRENT_TIMESTAMP AS snapshot_timestamp
FROM 
    product
WHERE 
    product_qty_type = 'unit';

-- 2. Insert a new unit product with current timestamp
INSERT INTO product_units (
    product_id,
    product_name,
    product_size,
    product_category_id,
    product_qty_type,
    snapshot_timestamp
)
VALUES (
    (SELECT COALESCE(MAX(product_id), 0) + 1 FROM product_units), -- Auto-increment ID
    'Gourmet Apple Pie', -- New product name
    '10 inch', -- Product size
    (SELECT product_category_id FROM product WHERE product_name LIKE '%Pie%' LIMIT 1), -- Matching category
    'unit', -- Quantity type
    CURRENT_TIMESTAMP -- Current timestamp
);

-- DELETE
/* 1. Delete the older record for the whatever product you added. */
DELETE FROM product_units
WHERE product_id = (
    SELECT product_id 
    FROM product_units 
    WHERE product_name = 'Gourmet Apple Pie'
    ORDER BY snapshot_timestamp ASC
    LIMIT 1
);

-- UPDATE
/* 1. Add current_quantity to product_units and update with last quantity from vendor_inventory */

-- First add the column
ALTER TABLE product_units
ADD current_quantity INT;

-- Then update with the last quantity values
UPDATE product_units
SET current_quantity = COALESCE(
    (SELECT quantity 
     FROM vendor_inventory vi 
     WHERE vi.product_id = product_units.product_id
     ORDER BY market_date DESC, vendor_id DESC
     LIMIT 1),
    0
);
