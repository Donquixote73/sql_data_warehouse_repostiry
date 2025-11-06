/*
 Description:
    This script creates the BRONZE layer tables in the data warehouse.
    It drops the tables if they already exist and then creates them with the
    appropriate structure. These tables are intended to store raw data
    from CRM and ERP sources.

    Tables created:
    - bronze.crm_cust_info      : Customer information from CRM
    - bronze.crm_prd_info       : Product information from CRM
    - bronze.crm_sales_details  : Sales details from CRM
    - bronze.erp_cust_az12      : Customer data from ERP
    - bronze.erp_local_a101     : Location data from ERP
    - bronze.erp_px_catg1v2    : Product category data from ERP

    Notes:
    - Adjust data types if necessary to match your source data.
    - This script is intended for demonstration or as the initial setup
      of the BRONZE layer in the ETL pipeline.
*/
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
 cid VARCHAR(50),
 bdate DATE,
 gen VARCHAR(50)
);
DROP TABLE IF EXISTS bronze.erp_local_a101;
CREATE TABLE bronze.erp_local_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50)
);
DROP TABLE IF EXISTS bronze.erp_px_catg1v2;
CREATE TABLE bronze.erp_px_catg1v2 (
	cat_id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
)




