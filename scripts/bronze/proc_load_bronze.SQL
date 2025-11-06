/*
    Description:
    This stored procedure loads raw data into the BRONZE layer tables of the data warehouse.
    It performs the following steps for each table:
      1. Truncates the table to remove old data.
      2. Loads data from a CSV file using the COPY command.
      3. Measures and logs the time taken to load each table individually.
    After all tables are loaded, it logs the total execution time for the procedure.

    Tables involved:
    - bronze.crm_cust_info
    - bronze.crm_prd_info
    - bronze.crm_sales_details
    - bronze.erp_cust_az12
    - bronze.erp_loc_a101
    - bronze.erp_px_cat_g1v2
    Usage Example:
    CALL bronze.load_bronze()
    Notes:
    - Replace CSV file paths with your local or server paths as needed.
    - Any errors during a table load are logged but do not stop the procedure from loading other tables.
    - Useful for ETL pipeline automation and monitoring data load times in the BRONZE layer.
*/
DROP PROCEDURE IF EXISTS bronze.load_bronze;

CREATE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    total_start TIMESTAMP;
    total_end TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    total_start := now();
    RAISE NOTICE 'LOADING BRONZE LAYER...';

    -- crm_cust_info
    BEGIN
        start_time := now();
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM '/path/to/cust_info.csv'
        DELIMITER ',' CSV HEADER;
        end_time := now();
        RAISE NOTICE 'crm_cust_info loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in crm_cust_info: %', SQLERRM;
    END;

    -- crm_prd_info
    BEGIN
        start_time := now();
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM '/path/to/prd_info.csv'
        DELIMITER ',' CSV HEADER;
        end_time := now();
        RAISE NOTICE 'crm_prd_info loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in crm_prd_info: %', SQLERRM;
    END;

    -- crm_sales_details
    BEGIN
        start_time := now();
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
        FROM '/path/to/sales_details.csv'
        DELIMITER ',' CSV HEADER;
        end_time := now();
        RAISE NOTICE 'crm_sales_details loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in crm_sales_details: %', SQLERRM;
    END;

    -- erp_cust_az12
    BEGIN
        start_time := now();
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM '/path/to/cust_az12.csv'
        DELIMITER ',' CSV HEADER;
        end_time := now();
        RAISE NOTICE 'erp_cust_az12 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in erp_cust_az12: %', SQLERRM;
    END;

    -- erp_loc_a101
    BEGIN
        start_time := now();
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM '/path/to/loc_a101.csv'
        DELIMITER ',' CSV HEADER;
        end_time := now();
        RAISE NOTICE 'erp_loc_a101 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in erp_loc_a101: %', SQLERRM;
    END;

    -- erp_px_cat_g1v2
    BEGIN
        start_time := now();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM '/path/to/px_cat_g1v2.csv'
        DELIMITER ',' CSV HEADER;
        end_time := now();
        RAISE NOTICE 'erp_px_cat_g1v2 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in erp_px_cat_g1v2: %', SQLERRM;
    END;

    total_end := now();
    RAISE NOTICE 'ALL BRONZE TABLES LOADED SUCCESSFULLY';
    RAISE NOTICE 'TOTAL LOAD TIME: %', total_end - total_start;
END;
$$;
