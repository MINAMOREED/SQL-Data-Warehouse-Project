USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @bach_start_time DATETIME, @bach_end_time DATETIME;

    BEGIN TRY
        SET @bach_start_time = GETDATE();
        PRINT '=============================';
        PRINT 'Loading Bronze Layer';
        PRINT '=============================';

        PRINT '------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>>Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT 'Inserting Data into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '------------------------------';

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>>Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT 'Inserting Data into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '------------------------------';

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>>Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT 'Inserting Data into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '------------------------------';

        PRINT '------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------';

        -- erp_CUST_AZ12
        SET @start_time = GETDATE();
        PRINT '>>Truncating table: bronze.erp_CUST_AZ12';
        TRUNCATE TABLE bronze.erp_CUST_AZ12;
        PRINT 'Inserting Data into: bronze.erp_CUST_AZ12';
        BULK INSERT bronze.erp_CUST_AZ12
        FROM '/var/opt/mssql/data/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '------------------------------';

        -- erp_LOC_A101
        SET @start_time = GETDATE();
        PRINT '>>Truncating table: bronze.erp_LOC_A101';
        TRUNCATE TABLE bronze.erp_LOC_A101;
        PRINT 'Inserting Data into: bronze.erp_LOC_A101';
        BULK INSERT bronze.erp_LOC_A101
        FROM '/var/opt/mssql/data/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '------------------------------';

        -- erp_PX_CAT_G1V2
        SET @start_time = GETDATE();
        PRINT '>>Truncating table: bronze.erp_PX_CAT_G1V2';
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
        PRINT 'Inserting Data into: bronze.erp_PX_CAT_G1V2';
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        PRINT '------------------------------';

        SET @bach_end_time = GETDATE();
        PRINT '=============================';
        PRINT 'Bronze Layer Loaded Successfully';
        PRINT '     -Total load duration: ' + CAST(DATEDIFF(SECOND , @bach_start_time, @bach_end_time)AS VARCHAR) + 'secound'
        PRINT '=============================';

    END TRY
    BEGIN CATCH
        PRINT '======================================================';
        PRINT 'Error occurred during loading bronze layer';
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error state: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '======================================================';
    END CATCH
END
GO

-- تشغيل الـ Procedure بعد إنشائها
EXEC bronze.load_bronze;
GO
