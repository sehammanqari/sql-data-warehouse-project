/* ============================================================
   DDL Script    : Gold Layer Views Creation Script
   Purpose       :
       This script creates the Gold layer views in the
       Data_Warehouse database.

       The Gold layer represents the business-ready data model
       used for analytics and reporting. It includes:
       - Dimension views for descriptive entities
       - Fact views for measurable business events

       Views created:
       1. gold.dim_customers
       2. gold.dim_products
       3. gold.fact_sales

   Usage         :
       - Run this script after the Silver layer tables are loaded.
       - These views can be used directly for reporting,
         dashboarding, and analytical queries.
       - The fact view connects to the dimension views using
         surrogate keys for easier analysis.

   Notes         :
       - gold.dim_customers: customer dimension
       - gold.dim_products : product dimension
       - gold.fact_sales   : sales fact table
   ============================================================ */

USE Data_Warehouse;
GO

/* ============================================================
   View: gold.dim_customers
   Purpose:
   Collect all customer-related attributes from the Silver layer,
   combine CRM and ERP data, and present them in a business-friendly
   dimension view.
   ============================================================ */
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    el.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
        ELSE ISNULL(ec.gen, 'n/a')
    END AS gender,
    ec.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ec
    ON ci.cst_key = ec.cid
LEFT JOIN silver.erp_loc_a101 AS el
    ON ci.cst_key = el.cid;
GO


/* ============================================================
   View: gold.dim_products
   Purpose:
   Collect product attributes from the Silver layer, combine
   product master data with category information, and expose
   the current active products as a business-friendly dimension.
   ============================================================ */
CREATE OR ALTER VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pf.prd_start_dt, pf.prd_key) AS product_key,
    pf.prd_id AS product_id,
    pf.prd_key AS product_number,
    pf.prd_nm AS product_name,
    pf.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance_required,
    pf.prd_cost AS cost,
    pf.prd_line AS product_line,
    pf.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pf
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pf.cat_id = pc.id
WHERE pf.prd_end_dt IS NULL;
GO


/* ============================================================
   View: gold.fact_sales
   Purpose:
   Create the sales fact view by combining transactional sales
   data with surrogate keys from the customer and product
   dimensions.
   ============================================================ */
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
    cs.sls_ord_num AS order_number,
    gp.product_key,
    gc.customer_key,
    cs.sls_order_dt AS order_date,
    cs.sls_ship_dt AS shipping_date,
    cs.sls_due_dt AS due_date,
    cs.sls_sales AS sales_amount,
    cs.sls_quantity AS quantity,
    cs.sls_price AS price
FROM silver.crm_sales_details AS cs
LEFT JOIN gold.dim_products AS gp
    ON cs.sls_prd_key = gp.product_number
LEFT JOIN gold.dim_customers AS gc
    ON cs.sls_cust_id = gc.customer_id;
GO
