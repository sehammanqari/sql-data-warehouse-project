/* =====================================================
   Data Warehouse Initialization Script
   Purpose:
   - Create Data_Warehouse database if it does not exist
   - Create bronze (Raw data), silver (Cleand data), and gold (Business ready data) schemas
   -----------------------------------------------------
   Warning:
   This script will NOT overwrite the database if it 
   already exists.
   ===================================================== */

-- Check if database exists
IF DB_ID('Data_Warehouse') IS NULL
BEGIN
    CREATE DATABASE Data_Warehouse;
    PRINT 'Database created.';
END
ELSE
BEGIN
    PRINT 'Database already exists.';
END
GO

-- Use the database
USE Data_Warehouse;
GO

-- Create bronze schema
IF SCHEMA_ID('bronze') IS NULL
    EXEC('CREATE SCHEMA bronze');
GO

-- Create silver schema
IF SCHEMA_ID('silver') IS NULL
    EXEC('CREATE SCHEMA silver');
GO

-- Create gold schema
IF SCHEMA_ID('gold') IS NULL
    EXEC('CREATE SCHEMA gold');
GO
