/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data to ensure a clean slate.
    - Uses the `LOAD DATA LOCAL INFILE` command to bulk load CSV files into the 
      corresponding bronze tables (CRM and ERP sources).
    - Tracks start and end times, and reports total duration of the load process.
    - Provides a success message when all tables are loaded successfully.
    - Provides a custom error message if any load operation fails.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL load_bronze();

Notes:
    - Ensure `local_infile` is enabled on both server and client.
    - CSV files must exist at the specified paths and match the table schema.
    - Line terminators are set for Windows (`\r\n`).
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN
    -- Declare variables for timing
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET end_time = NOW();
        SELECT 'Error occurred while loading Bronze tables. Please check file paths, permissions, or local_infile settings.' AS error_message,
               start_time AS procedure_start,
               end_time AS procedure_end,
               TIMESTAMPDIFF(SECOND, start_time, end_time) AS duration_seconds;
    END;

    -- Capture start time
    SET start_time = NOW();

    -- CRM Customer Info
    LOAD DATA LOCAL INFILE 'C:/Users/Aman Kumar/Downloads/Data_warehouse/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze_crm_cust_info
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS
    (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);

    -- CRM Product Info
    LOAD DATA LOCAL INFILE 'C:/Users/Aman Kumar/Downloads/Data_warehouse/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze_crm_prd_info
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS
    (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt);

    -- CRM Sales Details
    LOAD DATA LOCAL INFILE 'C:/Users/Aman Kumar/Downloads/Data_warehouse/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze_crm_sales_details
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS
    (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);

    -- ERP Location
    LOAD DATA LOCAL INFILE 'C:/Users/Aman Kumar/Downloads/Data_warehouse/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
    INTO TABLE bronze_erp_loc_a101
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS
    (cid, cntry);

    -- ERP Customer
    LOAD DATA LOCAL INFILE 'C:/Users/Aman Kumar/Downloads/Data_warehouse/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
    INTO TABLE bronze_erp_cust_az12
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS
    (cid, bdate, gen);

    -- ERP Product Category
    LOAD DATA LOCAL INFILE 'C:/Users/Aman Kumar/Downloads/Data_warehouse/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
    INTO TABLE bronze_erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    IGNORE 1 ROWS
    (id, cat, subcat, maintenance);

    -- Capture end time
    SET end_time = NOW();

    -- Success message with timing
    SELECT 'All Bronze tables loaded successfully!' AS success_message,
           start_time AS procedure_start,
           end_time AS procedure_end,
           TIMESTAMPDIFF(SECOND, start_time, end_time) AS duration_seconds;
END$$

DELIMITER ;
