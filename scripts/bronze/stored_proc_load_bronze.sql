/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files into bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc_start  DATETIME2(3) = SYSDATETIME();
    DECLARE @proc_end    DATETIME2(3);

    DECLARE @table_start DATETIME2(3);
    DECLARE @table_end   DATETIME2(3);

    BEGIN TRY

        PRINT '=======================================================';
        PRINT 'Loding The Bronze Layar'
        PRINT '=======================================================';

        PRINT '-------------------------------------------------------';
        PRINT 'Loding CRM Tables'
        PRINT '-------------------------------------------------------';

        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: bronze.crm_cust_info';
	        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
	        BULK INSERT bronze.crm_cust_info
	        FROM 'D:\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	        WITH(
	        FIRSTROW=2,
	        FIELDTERMINATOR = ',',
	        TABLOCK
	        );

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (bronze.crm_cust_info): ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';

        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: bronze.crm_prd_info';
	        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
	        BULK INSERT bronze.crm_prd_info
	        FROM 'D:\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	        WITH(
	        FIRSTROW=2,
	        FIELDTERMINATOR = ',',
	        TABLOCK
	        );

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (bronze.crm_prd_info): ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';

        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: bronze.crm_sales_details';
	        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
	        BULK INSERT bronze.crm_sales_details
	        FROM 'D:\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	        WITH(
	        FIRSTROW=2,
	        FIELDTERMINATOR = ',',
	        TABLOCK
	        );

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (bronze.crm_sales_details): ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';


        PRINT '-------------------------------------------------------';
        PRINT 'Loding ERP Tables'
        PRINT '-------------------------------------------------------';

        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
	        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
	        BULK INSERT  bronze.erp_cust_az12
	        FROM 'D:\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	        WITH(
	        FIRSTROW=2,
	        FIELDTERMINATOR = ',',
	        TABLOCK
	        );

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (bronze.erp_cust_az12): ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';

        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
	        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
	        BULK INSERT  bronze.erp_loc_a101
	        FROM 'D:\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	        WITH(
	        FIRSTROW=2,
	        FIELDTERMINATOR = ',',
	        TABLOCK
	        );

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (bronze.erp_loc_a101): ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';

        SET @table_start = SYSDATETIME();

        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
	        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	        BULK INSERT  bronze.erp_px_cat_g1v2
	        FROM 'D:\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	        WITH(
	        FIRSTROW=2,
	        FIELDTERMINATOR = ',',
	        TABLOCK
	        );

        SET @table_end = SYSDATETIME();
        PRINT '>> Duration (bronze.erp_px_cat_g1v2): ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @table_start, @table_end)) + ' sec';


        SET @proc_end = SYSDATETIME();
        PRINT '=======================================================';
        PRINT 'Bronze load completed successfully.';
        PRINT 'Total duration: ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @proc_start, @proc_end)) + ' sec';
        PRINT '=======================================================';

    END TRY
    BEGIN CATCH
        SET @proc_end = SYSDATETIME();

        PRINT '=======================================================';
        PRINT 'Bronze load FAILED.';
        PRINT 'Total duration before failure: ' 
              + CONVERT(VARCHAR(12), DATEDIFF(SECOND, @proc_start, @proc_end)) + ' sec';
        PRINT '-------------------------------------------------------';
        PRINT 'Error details:';
        PRINT '  Error Number  : ' + CAST(ERROR_NUMBER() AS VARCHAR(20));
        PRINT '  Severity      : ' + CAST(ERROR_SEVERITY() AS VARCHAR(20));
        PRINT '  State         : ' + CAST(ERROR_STATE() AS VARCHAR(20));
        PRINT '  Procedure     : ' + ISNULL(ERROR_PROCEDURE(), '(N/A)');
        PRINT '  Line          : ' + CAST(ERROR_LINE() AS VARCHAR(20));
        PRINT '  Message       : ' + ERROR_MESSAGE();
        PRINT '=======================================================';
    END CATCH
END
GO
