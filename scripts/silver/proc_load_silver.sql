/*
 Silver Layer Data Load Script
 This procedure loads data from the Bronze layer into the Silver layer tables.
 Transformations include:
   - Cleaning and trimming text fields
   - Standardizing categorical values (e.g., gender, marital status, product lines)
   - Converting date and numeric fields to proper types
   - Deduplicating records based on latest available data
   - Handling missing or incorrect values with conditional logic
Usage example: CALL silver.load_silver()
 This prepares the data for analytics and further processing in the Gold layer.
*/

DROP PROCEDURE IF EXISTS silver.load_silver();
CREATE OR REPLACE PROCEDURE silver.load_silver() LANGUAGE plpgsql 
AS $$
	DECLARE
	    total_start TIMESTAMP;
	    total_end TIMESTAMP;
	    start_time TIMESTAMP;
	    end_time TIMESTAMP;
	BEGIN
	    total_start := now();
	    RAISE NOTICE 'LOADING SILVER LAYER...';
		BEGIN
			start_time := now();
			RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			RAISE NOTICE '>> Inserting Data Info: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info (
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
				TRIM(cst_firstname) AS cst_firstname ,
				TRIM(cst_lastname) AS cst_lastname,
				CASE 
					WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					ELSE 'n/a'
				END AS cst_marital_status,
				CASE 
					WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					ELSE 'n/a'
				END AS cst_gndr,
				cst_create_date
			FROM 
			(
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) t
			WHERE flag_last = 1;
			end_time:= now();
			RAISE NOTICE 'crm_cust_info loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in crm_cust_info: %', SQLERRM;
		END;
		
		BEGIN
		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		RAISE NOTICE '>> Inserting Data Info: silver.crm_prd_info';
		
		INSERT INTO silver.crm_prd_info (
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
			prd_id::INT AS prd_id ,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
			prd_nm,
			COALESCE(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST (prd_start_dt AS DATE) AS prd_start_dt,
			CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info ;
		end_time := now();
		RAISE NOTICE 'crm_prd_info loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in crm_prd_info: %', SQLERRM;
		END;

		BEGIN
		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		RAISE NOTICE '>> Inserting Data Info: silver.crm_sales_details';
		
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
		sls_cust_id::int,
			CASE WHEN sls_order_dt = '0' OR LENGTH(sls_order_dt)<>8 THEN NULL
				 ELSE CAST(sls_order_dt AS DATE) 
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt= '0' OR LENGTH(sls_ship_dt)<>8 THEN NULL
				 ELSE CAST(sls_ship_dt AS DATE) 
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt= '0' OR LENGTH(sls_due_dt)<>8 THEN NULL
				 ELSE CAST(sls_due_dt AS DATE) 
			END AS sls_due_dt,
			
			CASE WHEN sls_sales IS NULL OR sls_sales::int <=0 
				 OR (sls_sales ::int) <> (sls_quantity::int)* ABS(sls_price::int)
				 THEN (sls_quantity::int) * ABS(sls_price::int) 
				 ELSE sls_sales ::int
			END AS sls_sales,
			sls_quantity::int,
			CASE WHEN sls_price IS NULL OR sls_price::int <= 0
				 THEN sls_sales::int / NULLIF(sls_quantity::int,0)
				 ELSE sls_price ::int
			END AS sls_price
		FROM bronze.crm_sales_details;
		end_time := now();
		RAISE NOTICE 'crm_sales_details loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in crm_sales_details: %', SQLERRM;
		END;

		BEGIN
		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		RAISE NOTICE '>> Inserting Data Info: silver.erp_cust_az12';
		
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			 ELSE cid
		END cid,
		CASE WHEN bdate::date > CURRENT_DATE THEN NULL
			 ELSE bdate::date
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12;
		end_time:= now();
		RAISE NOTICE 'erp_cust_az12 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in erp_cust_az12: %', SQLERRM;
		END;

		BEGIN
		start_time:= now();
		RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		RAISE NOTICE '>> Inserting Data Info: silver.erp_loc_a101';
		
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		SELECT
		REPLACE(cid,'-','') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101;
		end_time := now();
		RAISE NOTICE 'erp_loc_a101 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in erp_loc_a101: %', SQLERRM;
		END;

		BEGIN
		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.erp_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE NOTICE '>> Inserting Data Info: silver.erp_cat_g1v2';
		
		INSERT INTO silver.erp_px_cat_g1v2 (
		cat_id,
		cat,
		subcat,
		maintenance)
		SELECT 
		cat_id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		end_time := now();
		RAISE NOTICE 'erp_cat_g1v2 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error in erp_cat_g1v2: %', SQLERRM;
		END;
	total_end := now();
    RAISE NOTICE 'ALL SILVER TABLES LOADED SUCCESSFULLY';
    RAISE NOTICE 'TOTAL LOAD TIME: %', total_end - total_start;
	END;
$$;
	
	
