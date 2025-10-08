/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  v_start_time  timestamptz;
  v_end_time    timestamptz;
  v_batch_start timestamptz;
  v_batch_end   timestamptz;

  -- Adjust these paths to wherever your CSVs are mounted inside the *db* container
  v_crm_path   text := '/datasets/source_crm';
  v_erp_path   text := '/datasets/source_erp';
BEGIN
  v_batch_start := now();
  RAISE NOTICE '================================================';
  RAISE NOTICE 'Loading Bronze Layer';
  RAISE NOTICE '================================================';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading CRM Tables';
  RAISE NOTICE '------------------------------------------------';

  -- crm_cust_info
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
  TRUNCATE TABLE bronze.crm_cust_info;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
  EXECUTE format($f$
    COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    FROM %L WITH (FORMAT csv, HEADER true)
  $f$, v_crm_path || '/cust_info.csv');

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- crm_prd_info
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
  TRUNCATE TABLE bronze.crm_prd_info;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
  EXECUTE format($f$
    COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    FROM %L WITH (FORMAT csv, HEADER true)
  $f$, v_crm_path || '/prd_info.csv');

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- crm_sales_details
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
  TRUNCATE TABLE bronze.crm_sales_details;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
  EXECUTE format($f$
    COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    FROM %L WITH (FORMAT csv, HEADER true)
  $f$, v_crm_path || '/sales_details.csv');

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading ERP Tables';
  RAISE NOTICE '------------------------------------------------';

  -- erp_loc_a101
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
  TRUNCATE TABLE bronze.erp_loc_a101;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
  EXECUTE format($f$
    COPY bronze.erp_loc_a101 (cid, cntry)
    FROM %L WITH (FORMAT csv, HEADER true)
  $f$, v_erp_path || '/loc_a101.csv');

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- erp_cust_az12
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
  TRUNCATE TABLE bronze.erp_cust_az12;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
  EXECUTE format($f$
    COPY bronze.erp_cust_az12 (cid, bdate, gen)
    FROM %L WITH (FORMAT csv, HEADER true)
  $f$, v_erp_path || '/cust_az12.csv');

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- erp_px_cat_g1v2
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
  EXECUTE format($f$
    COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    FROM %L WITH (FORMAT csv, HEADER true)
  $f$, v_erp_path || '/px_cat_g1v2.csv');

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  v_batch_end := now();
  RAISE NOTICE '==========================================';
  RAISE NOTICE 'Loading Bronze Layer is Completed';
  RAISE NOTICE '   - Total Load Duration: % seconds',
               extract(epoch FROM (v_batch_end - v_batch_start))::int;
  RAISE NOTICE '==========================================';

EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error: %', SQLERRM;
    RAISE NOTICE '==========================================';
    RAISE; -- rethrow to fail the pipeline
END;
$$;