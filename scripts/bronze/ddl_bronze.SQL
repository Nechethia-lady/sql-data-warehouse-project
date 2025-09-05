
--Create SQL DDL scripts for all CSV files in the CRM and ERP systems


IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info
create table bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR (50),
	cst_firstname NVARCHAR (50),
	cst_lastname NVARCHAR (50),
	cst_marital_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info
create table bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
	);

IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR (50),
	sls_prd_key NVARCHAR (50),
	sls_cust_id INT,
	sls_order_dt INT, 
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
	);


IF OBJECT_ID ('bronze.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12(
	CID NVARCHAR (50),
	BDATE DATE,
	GEN NVARCHAR(50)
	);


IF OBJECT_ID ('bronze.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101(
CID NVARCHAR (50),
CNTRY NVARCHAR (50)
);


IF OBJECT_ID ('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR (50),
cat NVARCHAR (50),
subcat NVARCHAR (50),
maintenance NVARCHAR (50)
);



--NEXT, WRITE SQL BULK INSERT TO LOAD ALL CSV FILES INTO THE BRONZE TABLES
--BUILD BRONZE LAYER BY CREATING STORED PROCEDURE
--Calculate the loading time for each of the scripts

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '===========================================================';
		PRINT 'Loading bronze layer';
		PRINT '===========================================================';
	
		PRINT '===========================================================';
		PRINT 'Loading CRM tables';
		PRINT '===========================================================';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\HP\Downloads\SQL data Warehouse Project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT '======================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		PRINT '>> Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\HP\Downloads\SQL data Warehouse Project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\HP\Downloads\SQL data Warehouse Project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();


		PRINT '===========================================================';
		PRINT 'Loading ERP tables';
		PRINT '===========================================================';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\HP\Downloads\SQL data Warehouse Project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\HP\Downloads\SQL data Warehouse Project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\HP\Downloads\SQL data Warehouse Project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	END TRY
	BEGIN CATCH
		PRINT '===========================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================================';
	END CATCH
END

--NEXT IS THE QUALITY CHECK

SELECT * FROM bronze.crm_cust_info;
SELECT COUNT (*) FROM bronze.erp_px_cat_g1v2;






