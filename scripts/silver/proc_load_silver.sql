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

create or alter procedure silver.load_silver AS
BEGIN
	DECLARE @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Loading Silver Layer';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Loading CRM Tables';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>';

		--Loading CRM tables

		SET @start_time = getdate();
		PRINT '>> TRUNCATE TABLE: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Inserting Date into: silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		select cst_id, 
		cst_key, 
		TRIM (cst_firstname) cst_firstname, 
		TRIM(cst_lastname) cst_lastname, 
		CASE WHEN UPPER (TRIM (cst_marital_status)) = 'S' then 'Single'
				WHEN UPPER (TRIM (cst_marital_status)) = 'M' then 'Married'
				ELSE 'NA'
		End cst_marital_status, 

		CASE WHEN UPPER (TRIM (cst_gndr)) = 'F' then 'Female'
				WHEN UPPER (TRIM (cst_gndr)) = 'M' then 'Male'
				ELSE 'NA'
		End cst_gndr, 

		cst_create_date
		from (
		select*,
		ROW_NUMBER () over (partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		) t where flag_last = 1 and cst_id is not null
		;
		SET @end_time = getdate();
		PRINT '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		SET @start_time = getdate();
		PRINT '>> TRUNCATE TABLE: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> Inserting Date into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		select
		prd_id,
		REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN (prd_key)) as prd_key,
		prd_nm,
		isnull (prd_cost, 0) prd_cost,

		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'NA'
		End as prd_line,
		CAST (prd_start_dt as date) as prd_start_dt,
		CAST (lead (prd_start_dt) over (partition by prd_key order by prd_start_dt ASC)-1 as DATE) as prd_end_dt
		from bronze.crm_prd_info;
		SET @end_time = getdate();
		PRINT '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		SET @start_time = getdate();
		PRINT '>> TRUNCATE TABLE: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting Date into: silver.crm_sales_details';

		insert into silver.crm_sales_details(sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt =0 or len(sls_order_dt) != 8 Then NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END as sls_order_dt,
		CASE WHEN sls_ship_dt =0 or len(sls_ship_dt) != 8 Then NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END as sls_ship_dt,
		CASE WHEN sls_due_dt =0 or len(sls_due_dt) != 8 Then NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END as sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity *  abs(sls_price)
				then sls_quantity * abs(sls_price)
				else sls_sales
		end  as sls_sales, 
		sls_quantity,
		case when sls_price is null or sls_price <= 0
				then sls_sales/ nullif(sls_quantity, 0)
				else sls_price 
		end  as sls_price
		from bronze.crm_sales_details;
		SET @end_time = getdate();
		PRINT '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		--Loading ERP Tables

		SET @start_time = getdate();
		PRINT '>> TRUNCATE TABLE: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> Inserting Date into: silver.erp_cust_az12';

		insert into silver.erp_cust_az12 (CID, BDATE, GEN)
		select 
		case 
			when CID like 'NAS%' then substring(CID, 4, LEN(CID)) 
				else CID
		end CID,
		case 
			when BDATE > GETDATE() then null
				else BDATE
			end as BDATE, 
		case 
			when UPPER(TRIM(GEN)) IN ('F', 'FEMALE') then 'Female'
			 when UPPER(TRIM(GEN)) IN ('M', 'MALE') then 'Male'
			 else 'N/A'
		end GEN 
		from bronze.erp_cust_az12;
		SET @end_time = getdate();
		PRINT '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		SET @start_time = getdate();
		PRINT '>> TRUNCATE TABLE: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting Date into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101 (CID, CNTRY)
		select
		replace (CID, '-', '') CID,
		CASE 
			 WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			 WHEN TRIM(CNTRY) in ('US', 'USA') THEN 'United States'
			 WHEN TRIM(CNTRY) IS NULL THEN 'N/A'
			 WHEN TRIM(CNTRY) = ' ' THEN 'N/A'
			 ELSE TRIM(CNTRY)
		END CNTRY
		from bronze.erp_loc_a101 
		SET @end_time = getdate();
		PRINT '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		SET @start_time = getdate();
		PRINT '>> TRUNCATE TABLE: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Inserting Date into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2 (id,
		cat,
		subcat,
		maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = getdate();
		PRINT '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';


		SET @batch_start_time = getdate();
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT '  -Total load duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>';

	END TRY
	BEGIN CATCH
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>'
		PRINT 'Error Ocurred During Loading Silver Layer'
		PRINT 'Error Message' + ERROR_MESSAGE ()
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>';
	END CATCH
END
