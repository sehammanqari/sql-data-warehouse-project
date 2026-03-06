/* ============================================================
   DATA QUALITY CHECKS + SILVER LOAD LOGIC d.
   ============================================================ */

--------------------------------------------------------------------------------
-- TABLE: bronze.crm_cust_info  -->  silver.crm_cust_info
--------------------------------------------------------------------------------
-- 1) Data Quality: PK duplicates / NULLs
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2) Data Quality: Unwanted spaces
SELECT cst_firstname, cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
   OR cst_lastname  <> TRIM(cst_lastname);

-- 3) Standardization & Consistency checks
SELECT DISTINCT(cst_gndr)  ---- 3 results: M,F,NULL
FROM bronze.crm_cust_info;

SELECT DISTINCT(cst_marital_status) ---- 3 results: S,M,NULL
FROM bronze.crm_cust_info;

-- SOLUTION: Insert cleaned data into Silver
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank_
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE rank_ = 1;

-----------------------------------------------------------------------------
-- AFTER INSERT: Re-check Silver table (Expected: no results)
-----------------------------------------------------------------------------

------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------
-- TABLE: bronze.crm_prd_info  -->  silver.crm_prd_info
------------------------------------------------------------------------------------------------

-- 1) Data Quality: PK duplicates / NULLs
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 2) Derivation check: split prd_key into cat_id + prd_key
SELECT
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info;

-- 3) Data Quality: Unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- 4) Data Quality: NULLs or negative values (prd_cost)
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 5) Standardization & Consistency checks (prd_line)
SELECT DISTINCT(prd_line) ---- 5 results: M,R,S,T,NULL
FROM bronze.crm_prd_info;

-- 6) Data Quality: invalid date order
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- SOLUTION: Modify DDL then insert cleaned data into Silver
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
  DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Then insert
INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END prd_line,
    CAST(prd_start_dt AS DATE),
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;



-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------
-- TABLE: bronze.crm_sales_details  -->  silver.crm_sales_details
-------------------------------------------------------------------------------------------------
-- 1) Data Quality: unwanted spaces
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

-- 2) Data Quality: invalid date formats/ranges (order/ship/due)
SELECT NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details    -- ** CUZ WE CAN'T CAST IT TO DATE IF IT WAS 0 OR NEGATIVR INT
WHERE sls_order_dt <=0 OR LEN(sls_order_dt)<>8 OR sls_order_dt >20500101 OR sls_order_dt<19000101;

SELECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <=0 OR LEN(sls_ship_dt)<>8 OR sls_ship_dt >20500101 OR sls_ship_dt<19000101;

SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <=0 OR LEN(sls_due_dt)<>8 OR sls_due_dt >20500101 OR sls_due_dt<19000101;

-- Order Date must always be earlier than the Shipping Date or Due Date.
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- 3) Data Consistency: Sales, Quantity, Price
/* Rules
If Sales is negative, zero, or null, derive it using Quantity and Price.
If Price is zero or null, calculate it using Sales and Quantity.
If Price is negative, convert it to a positive value.
*/
SELECT
    sls_sales AS OLDsls_sales,
    sls_quantity,
    sls_price AS OLDsls_price,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales<> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END sls_sales,
    CASE
        WHEN sls_price IS NULL OR sls_price <=0
            THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END sls_price
FROM bronze.crm_sales_details;

-- SOLUTION: Modify DDL first, then insert cleaned data into Silver
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
        WHEN sls_order_dt=0 OR LEN(sls_order_dt)<>8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)<>8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt=0 OR LEN(sls_due_dt)<>8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
    END AS sls_due_dt,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales<> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <=0
            THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_sales_details;



------------------------------------------------------------------------------------

------------------------------------------------------------------------------------
-- TABLE: bronze.erp_cust_az12  -->  silver.erp_cust_az12
------------------------------------------------------------------------------------
-- 1) Clean CID
SELECT
    cid AS OLD,
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid
    END AS cid
FROM bronze.erp_cust_az12;

-- 2) Data Quality: invalid dates
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- 3) Standardization & Consistency
SELECT DISTINCT(gen) AS old,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'n/a'
    END gen
FROM bronze.erp_cust_az12;

-- SOLUTION: Insert cleaned data into Silver
INSERT INTO silver.erp_cust_az12(
    cid, bdate, gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid
    END AS cid,
    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END bdate,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'n/a'
    END gen
FROM bronze.erp_cust_az12;

SELECT *
FROM [silver].[erp_cust_az12];



--------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------
-- TABLE: bronze.erp_loc_a101  -->  silver.erp_loc_a101
--------------------------------------------------------------------------------------
-- 1) Clean PK format
SELECT
    cid AS OLD,
    REPLACE(cid,'-','') AS cid
FROM bronze.erp_loc_a101;

-- 2) Standardization & Consistency
SELECT DISTINCT(cntry) AS OLD,
    CASE
        WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
        WHEN TRIM(cntry)='DE' THEN 'Germany'
        WHEN TRIM(cntry)='' OR TRIM(cntry) IS NULL THEN 'n/a'
        ELSE cntry
    END AS cntry
FROM bronze.erp_loc_a101;

-- SOLUTION: Insert cleaned data into Silver
INSERT INTO silver.erp_loc_a101(
    cid, cntry
)
SELECT
    REPLACE(cid,'-','') AS cid,
    CASE
        WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
        WHEN TRIM(cntry)='DE' THEN 'Germany'
        WHEN TRIM(cntry)='' OR TRIM(cntry) IS NULL THEN 'n/a'
        ELSE cntry
    END AS cntry
FROM bronze.erp_loc_a101;

SELECT *
FROM silver.erp_loc_a101;



----------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------
-- TABLE: bronze.erp_px_cat_g1v2  -->  silver.erp_px_cat_g1v2
----------------------------------------------------------------------------------------
-- 1) Data Quality: unwanted spaces
SELECT
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) OR subcat <> TRIM(subcat) OR maintenance <> TRIM(maintenance);

-- 2) Standardization & Consistency
SELECT DISTINCT(cat)
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT(subcat)
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT(maintenance)
FROM bronze.erp_px_cat_g1v2;

-- SOLUTION: Insert cleaned data into Silver
INSERT INTO silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
