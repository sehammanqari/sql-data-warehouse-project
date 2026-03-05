/* ============================================================
   Purpose       :
       Load and transform data from the Bronze layer into the
       Silver layer. This procedure standardizes values, trims
       text, fixes basic inconsistencies, and prepares curated
       Silver tables for downstream use (Gold / analytics).

   WARNING / HEADS-UP:
       - This procedure TRUNCATES the Silver tables listed below
         before inserting fresh data.
       - Run only when you intend to fully reload the Silver layer.
       - If the Bronze source tables are empty or missing expected
         fields, the load will fail.
       - Errors are re-thrown (THROW) so jobs/pipelines can detect
         failure correctly.
   ============================================================ */

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc_start  DATETIME2(3) = SYSDATETIME();
    DECLARE @proc_end    DATETIME2(3);
    DECLARE @table_start DATETIME2(3);
    DECLARE @table_end   DATETIME2(3);

    BEGIN TRY

        PRINT '=======================================================';
        PRINT 'Loading The Silver Layer';
        PRINT '=======================================================';

        /* =========================
           silver.crm_cust_info
           ========================= */
        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
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
            TRIM(cst_lastname)  AS cst_lastname,
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

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (silver.crm_cust_info): '
            + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';



        /* =========================
           silver.crm_prd_info
           ========================= */
        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
            SUBSTRING(prd_key,7,LEN(prd_key))       AS prd_key,
            prd_nm,
            ISNULL(prd_cost,0)                      AS prd_cost,
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

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (silver.crm_prd_info): '
            + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';



        /* =========================
           silver.crm_sales_details
           ========================= */
        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (silver.crm_sales_details): '
            + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';



        /* =========================
           silver.erp_cust_az12
           ========================= */
        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
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

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (silver.erp_cust_az12): '
            + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';



        /* =========================
           silver.erp_loc_a101
           ========================= */
        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(
            cid, cntry
        )
        SELECT
            REPLACE(cid,'-','') AS cid,
            CASE
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
                ELSE cntry
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (silver.erp_loc_a101): '
            + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';



        /* =========================
           silver.erp_px_cat_g1v2
           ========================= */
        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(
            id, cat, subcat, maintenance
        )
        SELECT
            id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (silver.erp_px_cat_g1v2): '
            + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';



        SET @proc_end = SYSDATETIME();
        PRINT '=======================================================';
        PRINT 'Silver load completed successfully.';
        PRINT 'Total duration: ' + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @proc_start, @proc_end)) + ' sec';
        PRINT '=======================================================';

    END TRY
    BEGIN CATCH
        SET @proc_end = SYSDATETIME();

        PRINT '=======================================================';
        PRINT 'Silver load FAILED.';
        PRINT 'Total duration before failure: ' + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @proc_start, @proc_end)) + ' sec';
        PRINT '-------------------------------------------------------';
        PRINT 'Error details:';
        PRINT '  Error Number  : ' + CAST(ERROR_NUMBER() AS VARCHAR(20));
        PRINT '  Severity      : ' + CAST(ERROR_SEVERITY() AS VARCHAR(20));
        PRINT '  State         : ' + CAST(ERROR_STATE() AS VARCHAR(20));
        PRINT '  Procedure     : ' + ISNULL(ERROR_PROCEDURE(), '(N/A)');
        PRINT '  Line          : ' + CAST(ERROR_LINE() AS VARCHAR(20));
        PRINT '  Message       : ' + ERROR_MESSAGE();
        PRINT '=======================================================';

        THROW;
    END CATCH
END
GO
