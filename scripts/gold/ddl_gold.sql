/*
This commit introduces the Gold View Layer for the analytics model, including three curated tables optimized for reporting and BI consumption:

gold.fact_sales — central fact table containing cleaned and enriched sales transactions.

gold.dim_customers — customer dimension with standardized attributes for segmentation and analysis.

gold.dim_products — product dimension providing consistent product metadata for downstream dashboards.

These views apply data validation, type alignment, and business logic to deliver a trusted, analytics-ready data model.
  */
--CREATE DIMENSION: gold.dim_customer
CREATE VIEW  gold.dim_customer AS
SELECT DISTINCT row_number() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
        CASE
            WHEN ci.cst_gndr::text <> 'n/a'::text THEN ci.cst_gndr
            ELSE COALESCE(ca.gen, 'n/a'::character varying)
        END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
   FROM silver.crm_cust_info ci
     LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key::text = ca.cid::text
     LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key::text = la.cid::text
  ORDER BY (row_number() OVER (ORDER BY ci.cst_id)), ci.cst_id;
--CREATE DIMENSION: gold.dim_products
CREATE VIEW gold.dim_products AS
 SELECT row_number() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
   FROM silver.crm_prd_info pn
     LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id::text = pc.cat_id::text
  WHERE pn.prd_end_dt IS NULL;
--CREATE FACT: gold.fact_sales
CREATE VIEW gold.fact_sales AS
SELECT 
sls_ord_num AS order_number,
pr.product_key ,
cu.customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date ,
sls_sales AS sales_amount,
sls_quantity AS quantity,
sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number 
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
