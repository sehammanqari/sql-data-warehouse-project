/* ============================================================
   Purpose       :
       Create the Silver layer tables inside the Data_Warehouse
       database. The Silver layer stores cleaned/standardized
       data prepared from the Bronze layer.

   WARNING / HEADS-UP:
       - This script will CREATE tables under the [silver] schema.
       - If a table already exists, this script will NOT overwrite it.
         (It checks existence first.)
       - Run this after the database + schemas (bronze/silver/gold)
         are created.
   ============================================================ */

USE Data_Warehouse;
GO

/* ============================================================
   Ensure [silver] schema exists
   ============================================================ */
IF SCHEMA_ID('silver') IS NULL
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

/* ============================================================
   Create table: silver.crm_cust_info
   ============================================================ */
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NULL
BEGIN
    CREATE TABLE silver.crm_cust_info(
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_marital_status NVARCHAR(50),
        cst_gndr NVARCHAR(50),
        cst_create_date DATE,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
GO

/* ============================================================
   Create table: silver.crm_prd_info
   ============================================================ */
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NULL
BEGIN
    CREATE TABLE silver.crm_prd_info(
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost INT,
        prd_line NVARCHAR(50),
        prd_start_dt DATETIME,
        prd_end_dt DATETIME,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
GO

/* ============================================================
   Create table: silver.crm_sales_details
   ============================================================ */
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NULL
BEGIN
    CREATE TABLE silver.crm_sales_details(
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
GO

/* ============================================================
   Create table: silver.erp_cust_az12
   ============================================================ */
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NULL
BEGIN
    CREATE TABLE silver.erp_cust_az12(
        cid NVARCHAR(50),
        bdate DATE,
        gen NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
GO

/* ============================================================
   Create table: silver.erp_loc_a101
   ============================================================ */
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NULL
BEGIN
    CREATE TABLE silver.erp_loc_a101(
        cid NVARCHAR(50),
        cntry NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
GO

/* ============================================================
   Create table: silver.erp_px_cat_g1v2
   ============================================================ */
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NULL
BEGIN
    CREATE TABLE silver.erp_px_cat_g1v2(
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );
END
GO
