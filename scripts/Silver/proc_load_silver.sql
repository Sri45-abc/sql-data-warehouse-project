/*
====================================================================================================
Stored Procedure: Load Silver Layer(Bronze -> Silver)
====================================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) Process to Populate the silver
Schema tables from bronze schema.

    Actions Performed:
      - Truncate Silver tables.
      - Inserrts transformed and cleaned data bronze into silver tables.

Parameters:
  NONE.
  This stored procedure does not accpet any parameters or return any values.

Usage Example:
  EXEC Silver.load_silver
====================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '========================================================';
        PRINT 'Loading Silver Layer';
        PRINT '========================================================';

        PRINT '---------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------------------------';

        --Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) as cst_firstname,
            TRIM(cst_lastname) as cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_material_status,       -- Normalize marital status values to readable format
            CASE 
                WHEN cst_gndr = 'F' THEN 'Female'
                WHEN cst_gndr = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,       -- Normalize gender values to readable format
            cst_create_date
        from (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flaged_status
            from bronze.crm_cust_info
        )t WHERE flaged_status = 1;     -- Select the most recent recored per customer
        
        SET @end_time = GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'Seconds'; 

        PRINT '============================================================';

        --Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
            prd_id,
            cst_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,  --Extract Category ID
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,     --Extract Product ID
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,        -- Map Product line codes to descriptive values
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE)
        AS prd_end_dt          -- Calculate end date as one day before the next start date
        from bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'Seconds'; 


        --Loading silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '============================================================';

        PRINT '>> Truncating Table : crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
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
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,        -- checking the value and converting to date format
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,         -- checking the value and converting to date format
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,          -- checking the value and converting to date format
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,       -- Recalculating sales if original value is missing or incorrect
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0)
                ELSE sls_price
            END AS sls_price        -- Derive price if original value is invalid
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'Seconds'; 

        PRINT '---------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '---------------------------------------------------------';

        --Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))     -- remove 'NAS' prefix if present
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate, -- Set future birthdates to null
            CASE 
                WHEN UPPER(TRIM(REPLACE(REPLACE( gen,CHAR(13),''),CHAR(10),''))) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(REPLACE(REPLACE( gen,CHAR(13),''),CHAR(10),''))) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen      -- Normalize gender values and handle unknown cases
        FROM bronze.erp_cust_az12

        SET @end_time = GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'Seconds'; 

        PRINT '============================================================';

        --Loading silver.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )
        SELECT
            REPLACE(cid,'-','') AS cid,     -- Remove the invalid values
            CASE 
                WHEN TRIM(REPLACE(REPLACE( cntry,CHAR(13),''),CHAR(10),'')) = 'DE' THEN 'Germany'
                WHEN TRIM(REPLACE(REPLACE( cntry,CHAR(13),''),CHAR(10),'')) IN ('US','USA') THEN 'United States'
                WHEN TRIM(REPLACE(REPLACE( cntry,CHAR(13),''),CHAR(10),'')) = '' OR TRIM(REPLACE(REPLACE( cntry,CHAR(13),''),CHAR(10),'')) IS NULL
                THEN 'n/a'
                ELSE REPLACE(REPLACE( cntry,CHAR(13),''),CHAR(10),'')
            END as cntry        -- Normalize and handle missing or blank country codes
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'Seconds'; 

        PRINT '============================================================';

        --Loading silver.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
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
        from bronze.erp_px_cat_g1v2;   

        SET @end_time = GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'Seconds'; 

        SET @batch_end_time = GETDATE();
        PRINT '===================================================';
        PRINT 'Loading Silver Layer is completed';
        PRINT 'Total Load Duration: '+ CAST(DATEDIFF(SECOND,@batch_start_time , @batch_end_time) AS NVARCHAR)+ 'Seconds';
        PRINT '===================================================';
    END TRY
    BEGIN CATCH
    PRINT '===================================================';
    PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
    PRINT 'Error Message '+ ERROR_MESSAGE();
    PRINT 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error State '+ CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '===================================================';
    END CATCH

END
