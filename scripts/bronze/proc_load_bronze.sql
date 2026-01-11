/*
=====================================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================================================================================
Script Purpose;
  This stored procedure loads data into the 'bronze' schema from external CSV Files.
  It Performs the following activities:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' Command to load data from CSV files to bronze  tables.

Parameters:
  NONE.
  This stored procedure does not accept any parameters or return any values.

Ussage Example:
  EXEC bronze.load_bronze;
=====================================================================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME,@start_time DATETIME, @end_time DATETIME
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '==================================================';

        PRINT '---------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/tmp/SQL_with_Bara/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: '+ CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT '-------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/tmp/SQL_with_Bara/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: '+ CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT '-------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/tmp/SQL_with_Bara/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: '+ CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';

        PRINT '---------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '---------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data into Table: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/tmp/SQL_with_Bara/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: '+ CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT '-------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data into Table: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/tmp/SQL_with_Bara/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: '+ CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT '-------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/tmp/SQL_with_Bara/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: '+ CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
        PRINT '-------------------------------------------------------';

        SET @batch_end_time =GETDATE();
        PRINT '=========================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '  - Total Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) As NVARCHAR) + ' Seconds';
        PRINT '=========================================';
    END TRY
    BEGIN CATCH
        PRINT '======================================================='
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message'+ ERROR_MESSAGE();
        PRINT 'Error Number'+ CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state'+ CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '======================================================='
    END CATCH
END
