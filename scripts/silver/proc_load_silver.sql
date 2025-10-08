/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
  v_start_time  timestamptz;
  v_end_time    timestamptz;
  v_batch_start timestamptz;
  v_batch_end   timestamptz;
BEGIN
  v_batch_start := now();
  RAISE NOTICE '================================================';
  RAISE NOTICE 'Loading Silver Layer';
  RAISE NOTICE '================================================';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading CRM Tables';
  RAISE NOTICE '------------------------------------------------';

  -- silver.crm_cust_info
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
  TRUNCATE TABLE silver.crm_cust_info;

  RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
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
    trim(cst_firstname) AS cst_firstname,
    trim(cst_lastname)  AS cst_lastname,
    CASE upper(trim(cst_marital_status))
      WHEN 'S' THEN 'Single'
      WHEN 'M' THEN 'Married'
      ELSE 'n/a'
    END AS cst_marital_status,
    CASE upper(trim(cst_gndr))
      WHEN 'F' THEN 'Female'
      WHEN 'M' THEN 'Male'
      ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
  FROM (
    SELECT *,
           row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
  ) t
  WHERE flag_last = 1;

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- silver.crm_prd_info
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
  TRUNCATE TABLE silver.crm_prd_info;

  RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
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
    prd_id,
    replace(substr(prd_key, 1, 5), '-', '_') AS cat_id,         -- category ID
    substr(prd_key, 7)                           AS prd_key,     -- product key (rest of string)
    prd_nm,
    COALESCE(prd_cost, 0)                        AS prd_cost,
    CASE upper(trim(prd_line))
      WHEN 'M' THEN 'Mountain'
      WHEN 'R' THEN 'Road'
      WHEN 'S' THEN 'Other Sales'
      WHEN 'T' THEN 'Touring'
      ELSE 'n/a'
    END AS prd_line,
    prd_start_dt::date                           AS prd_start_dt,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::date
      AS prd_end_dt
  FROM bronze.crm_prd_info;

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- silver.crm_sales_details
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
  TRUNCATE TABLE silver.crm_sales_details;

  RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
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
      WHEN sls_order_dt = 0 OR length(sls_order_dt::text) <> 8 THEN NULL
      ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
    END AS sls_order_dt,
    CASE
      WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) <> 8 THEN NULL
      ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
    END AS sls_ship_dt,
    CASE
      WHEN sls_due_dt = 0 OR length(sls_due_dt::text) <> 8 THEN NULL
      ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
    END AS sls_due_dt,
    CASE
      WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * abs(sls_price)
        THEN sls_quantity * abs(sls_price)
      ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE
      WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
      ELSE sls_price
    END AS sls_price
  FROM bronze.crm_sales_details;

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading ERP Tables';
  RAISE NOTICE '------------------------------------------------';

  -- silver.erp_cust_az12
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
  TRUNCATE TABLE silver.erp_cust_az12;

  RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
  INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
  SELECT
    CASE
      WHEN cid LIKE 'NAS%' THEN substr(cid, 4)   -- remove 'NAS' prefix
      ELSE cid
    END AS cid,
    CASE
      WHEN bdate > now()::date THEN NULL
      ELSE bdate
    END AS bdate,
    CASE
      WHEN upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
      WHEN upper(trim(gen)) IN ('M','MALE')   THEN 'Male'
      ELSE 'n/a'
    END AS gen
  FROM bronze.erp_cust_az12;

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- silver.erp_loc_a101
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
  TRUNCATE TABLE silver.erp_loc_a101;

  RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
  INSERT INTO silver.erp_loc_a101 (cid, cntry)
  SELECT
    replace(cid, '-', '') AS cid,
    CASE
      WHEN trim(cntry) = 'DE'                THEN 'Germany'
      WHEN trim(cntry) IN ('US','USA')       THEN 'United States'
      WHEN trim(cntry) = '' OR cntry IS NULL THEN 'n/a'
      ELSE trim(cntry)
    END AS cntry
  FROM bronze.erp_loc_a101;

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  -- silver.erp_px_cat_g1v2
  v_start_time := now();
  RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
  TRUNCATE TABLE silver.erp_px_cat_g1v2;

  RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
  INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
  SELECT id, cat, subcat, maintenance
  FROM bronze.erp_px_cat_g1v2;

  v_end_time := now();
  RAISE NOTICE '>> Load Duration: % seconds', extract(epoch FROM (v_end_time - v_start_time))::int;
  RAISE NOTICE '>> -------------';

  v_batch_end := now();
  RAISE NOTICE '==========================================';
  RAISE NOTICE 'Loading Silver Layer is Completed';
  RAISE NOTICE '   - Total Load Duration: % seconds',
               extract(epoch FROM (v_batch_end - v_batch_start))::int;
  RAISE NOTICE '==========================================';

EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========================================';
    RAISE; -- rethrow to fail the pipeline
END;
$$;
