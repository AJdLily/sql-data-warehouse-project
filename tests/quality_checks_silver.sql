/*
==============================================================================================================
Quality Checks
==============================================================================================================
Script Purpose:
     This script performs various quality checks for data consistency, accuracy and
     standardization across the 'silver' schema. It includes checks for:
     - Null or Duplicate Primary Keys.
     - Unwanted Spaces in String Feilds.
     - Data Standardization and Consistency.
     - Invalid Date Ranges and Orders.
     - Data Consistency between Related Feilds.

Usage Notes:
     - Run these checks after data loading the Silver Layer.
     - Investigate and resolve and discrepancies found during the check.
=============================================================================================================
*/


SELECT * FROM bronze.crm_cust_info;

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- DATA TRANSFORMATION & CLEANING

SELECT 
* ,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;


-- DOUBLE CHECKING FOR NULLS & DUPS
-- PRIMARY KEY IS UNIQUE & EACH VALUE EXISTS FOR ONLY ONCE

SELECT
*
FROM(
SELECT 
* ,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;


-- CHECK FOR UNWANTED SPACES
-- EXPECTATION: NO RESULTS

SELECT
cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM (cst_firstname);

SELECT
cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM (cst_lastname);

SELECT
cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM (cst_gndr);

-----------------------------------------------------------------------------------------------------------------
-- RENAMED COLUMN 'cst_material_status' ---> 'cst_marital_status'.

EXEC sp_rename 
    'silver.crm_cust_info.cst_material_status',
    'cst_marital_status',
    'COLUMN';

SELECT * FROM silver.crm_cust_info


------------------------------------ QUALITY CHECK AFTER LOADING -------------------------------------------
------------------------------------ TABLE: silver.crm_cust_info -------------------------------------------

SELECT * FROM silver.crm_cust_info;

-- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- EXPECTATION: NO RESULT

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- DATA TRANSFORMATION & CLEANING

SELECT 
* ,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info
WHERE cst_id = 29466;


-- DOUBLE CHECKING FOR NULLS & DUPS
-- PRIMARY KEY IS UNIQUE & EACH VALUE EXISTS FOR ONLY ONCE

SELECT
*
FROM(
SELECT 
* ,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info
)t WHERE flag_last = 1;


-- CHECK FOR UNWANTED SPACES
-- EXPECTATION: NO RESULTS

SELECT
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM (cst_firstname);

SELECT
cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM (cst_lastname);

SELECT
cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM (cst_gndr);

SELECT * FROM silver.crm_cust_info;



------------------------------------ QUALITY CHECK BEFORE LOADING ------------------------------------------
------------------------------------ TABLE: bronze.crm_prd_info --------------------------------------------

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- EXPECTATION: NO RESULTS

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- CHECK FOR UNWANTED SPACES IN FIRST & LAST NAMES
-- EXPECTATIONS: NO RESULT

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM (prd_nm)

-- CHECK FOR NULLS & NEGATIVE NUMBERS IN COST
-- EXPECTATIONS: NO RESULTS

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- DATA STANDARDIZATION & CONSISTENCY
-- EXPECTATIONS: NO RESULTS

--SELECT cst_key
--FROM bronze.crm_cust_info
--WHERE cst_key != TRIM (cst_key)

SELECT prd_line
FROM bronze.crm_prd_info
GROUP BY prd_line

-- CHECK FOR INVALID DATE ORDERS

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt 

-- DATA TRANSFORMATIONS --

-- SPLITTING prd_key STRING INTO TWO STRINGS TO DERIVE TWO NEW COULUMNS
-- FIRST 5 CHARS ARE CATEGORY ID
-- REPLACING '-' IN prd_key from crm_prd_info with '_' SO THAT WE CAN JOIN DATA WITH id from erp_px_cat_g1v2
-- FILTERS OUT UNMATCHED DATA AFTER APPLYING TRANFORMATION

SELECT
	prd_id,
	prd_key,
	REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') NOT IN
(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2  --double checking so that we can join data together with outher source


-- second part of the string 
SELECT
	prd_id,
	prd_key,
	REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING (prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


-- Gonna join prd_key with sls_prd_key from bronze.crm_cust_info

SELECT
	prd_id,
	prd_key,
	REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING (prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING (prd_key, 7, LEN(prd_key)) IN
(SELECT sls_order_key FROM bronze.crm_sales_details)

SELECT sls_order_key FROM bronze.crm_sales_details  --double checking so that we can join data together with outher source


-- CONVERTING ALL NULLS IN prd_cost TO 0 
-- RENAMING ABBREVIATIONS IN prd_line

SELECT
	prd_id,
	prd_key,
	REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING (prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	ISNULL (prd_cost, 0) AS prd_cost,
	CASE UPPER( TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


-- MANDATORY: END DATE MUST NOT BE EARLIER THAN THE START DATE
-- SOL 1: SWITCH END DATE & START DATE
-- ISSUE: EACH RECORD MUST HAVE A START DATE!!
-- SOL 2: DERIVE THE END DATE FROM THE START DATE ---> END DATE = START DATE OF THE 'NEXT' RECORD - 1

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	LEAD ( prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')


------ FINAL -------

SELECT
	prd_id,
	REPLACE (SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract category id
	SUBSTRING (prd_key, 7, LEN(prd_key)) AS prd_key,  -- Extract product key
	prd_nm,
	ISNULL (prd_cost, 0) AS prd_cost,
	CASE UPPER( TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,  -- Map product line codes to descriptive value
	CAST (prd_start_dt AS DATE) AS prd_start_date,  
	CAST (
	      LEAD ( prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
		  AS DATE
		  ) AS prd_end_dt  -- Calculate end date as one day before the next start date
FROM bronze.crm_prd_info


------------------------------------ QUALITY CHECK AFTER LOADING -----------------------------------------
------------------------------------ TABLE: silver.crm_prd_info ------------------------------------------

-- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- EXPECTATION: NO RESULTS

SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- CHECK FOR UNWANTED SPACES 
-- EXPECTATIONS: NO RESULT

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM (prd_nm)

-- CHECK FOR NULLS & NEGATIVE NUMBERS IN COST
-- EXPECTATIONS: NO RESULTS

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- DATA STANDARDIZATION & CONSISTENCY
-- EXPECTATIONS: NO RESULTS

--SELECT cst_key
--FROM bronze.crm_cust_info
--WHERE cst_key != TRIM (cst_key)

SELECT prd_line
FROM silver.crm_prd_info
GROUP BY prd_line

-- CHECK FOR INVALID DATE ORDERS

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt 

-- A LOOK AT THE TABLE

SELECT * FROM silver.crm_prd_info;


------------------------------------ QUALITY CHECK BEFORE LOADING ----------------------------------------------
------------------------------------ TABLE: bronze.crm_sales_details -------------------------------------------

-- NOTE: COLUMN - sls_order_key FROM bronze.crm_sales_details | SAME | sls_order_key FROM silver.crm_sales_details

-- CHECKING FOR ISSUES
SELECT 
	   sls_order_num,
       sls_order_key,
       sls_cust_id,
       sls_order_dt,
       sls_ship_dt,
       sls_due_dt,
       sls_sales,
       sls_quantity,
       sls_price
FROM bronze.crm_sales_details
WHERE sls_order_num != TRIM (sls_order_num)

SELECT 
	   sls_order_num,
       sls_order_key,
       sls_cust_id,
       sls_order_dt,
       sls_ship_dt,
       sls_due_dt,
       sls_sales,
       sls_quantity,
       sls_price
FROM bronze.crm_sales_details
WHERE sls_order_key NOT IN (SELECT sls_order_key FROM silver.crm_sales_details)
                              -- OR -- SOMETHING WRONG
SELECT *
FROM bronze.crm_sales_details b
WHERE NOT EXISTS ( 
	SELECT 1
	FROM silver.crm_sales_details s
	WHERE s.sls_order_key = b.sls_order_key
);
-- CHECK LIKE THIS FOR EACH COULMN OF bronze.crm_sales_details WITH silver.crm_sales_details

SELECT 
	   sls_order_num,
       sls_order_key,
       sls_cust_id,
       sls_order_dt,
       sls_ship_dt,
       sls_due_dt,
       sls_sales,
       sls_quantity,
       sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
-- CHECK LIKE THIS FOR EACH COULMN OF bronze.crm_sales_details WITH silver.crm_sales_details


-- CHECK FOR INVALID DATES

SELECT 
NULLIF (sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

SELECT 
NULLIF (sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101

SELECT 
NULLIF (sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101

-- ORDER DATE MUST ALWAYS BE EARLIER THAN THE SHIPPING DATE OR DUE DATE
-- CHECK FOR INVALID DATES ORDERS

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- CHECK DATA CONSISTENCY BETWEEN: SALES, QUANTITY & PRICE
-- >> SALES = QUANTITY * PRICE
-- VALUES MUST NOT BE NULL, NEGATIVE OR 0

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price	
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- SOL1: Data issues will be fixed directly in source system
-- SOL2: Data issues have to be fixed in the data warehouse

-- RULE1: If Sales is negative, null or 0, derive it from the Quantity and Price
-- RULE2: If Price is 0 or null, calculate it using Sales and Quantity
-- RULE3: If Price is negative, convert it to a positive value

SELECT DISTINCT
	sls_sales AS old_sales,
	sls_quantity,
	sls_price AS old_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF (sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


------------------------------------ QUALITY CHECK AFTER LOADING ------------------------------------------------
------------------------------------ TABLE: bronze.crm_sales_details --------------------------------------------

-- CHECK FOR INVALID DATES ORDERS

SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details


------------------------------------ QUALITY CHECK BEFORE LOADING ----------------------------------------------
------------------------------------ TABLE: bronze.erp_cust_az12 -----------------------------------------------


--CHECKING HOW TO MATCH cid and cst_key
-- FOR Col: cid

SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%'


-- CHECKING MATCHING AGAIN
-- FOR Col: cid

SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
      ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- FOR Col: bdate
-- IDENTIFY OUT OF RANGE DATES
-- CHECK FOR VERY OLD CUSTOMERS
-- CHECK FOR BIRTHDAYS IN FUTURE

SELECT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' AND bdate > GETDATE()

SELECT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


-- DATA STANDARDIZATION & CONSISTENCY

SELECT gen
FROM bronze.erp_cust_az12

SELECT DISTINCT
	gen,
	CASE WHEN UPPER( TRIM (gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER( TRIM (gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12


------------------------------------ QUALITY CHECK AFTER LOADING ----------------------------------------
------------------------------------ TABLE: silver.erp_cust_az12 ----------------------------------------

-- DATA STANDARDIZATION & CONSISTENCY
SELECT gen
FROM silver.erp_cust_az12
GROUP BY gen

-- IDENTIFY OUT OF RANGE DATES
SELECT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- A LOOK AT THE TABLE
SELECT * FROM silver.erp_cust_az12


------------------------------------ QUALITY CHECK BEFORE LOADING ------------------------------------------------
------------------------------------ TABLE: bronze.erp_loc_a101 --------------------------------------------------

SELECT
cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;

-- FOR Col: cid
-- Removing '-'

SELECT
	REPLACE (cid, '-', '') AS cid,
	cntry
FROM bronze.erp_loc_a101;

-- Finding no unmatching data

SELECT
	REPLACE (cid, '-', '') AS cid,
	cntry
FROM bronze.erp_loc_a101
WHERE REPLACE (cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- FOR Col: cntry
-- DATA STANDARDIZATION & CONSISTENCY

SELECT DISTINCT 
cntry 
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT
	cntry AS old_cntry,
	CASE WHEN TRIM (cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM (cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM (cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM (cntry)
	END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry


------------------------------------ QUALITY CHECK AFTER LOADING ----------------------------------------
------------------------------------ TABLE: silver.erp_loc_a101 -----------------------------------------

-- DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT 
cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

 -- A LOOK AT THE TABLE
 SELECT * FROM silver.erp_loc_a101



------------------------------------ QUALITY CHECK BEFORE LOADING ----------------------------------------
------------------------------------ TABLE: silver.erp_px_cat_g1v2 ------------------------------------------

SELECT
id,
cat,
subcat,
maintainence
FROM bronze.erp_px_cat_g1v2

-- FOR Col:id ---> id matches exactly with cat_id From silver.crm_prd_info

-- FOR Col: cat, subcat & maintainence
-- CHECKING FOR UNWANTED SPACES

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM (cat) OR subcat != TRIM (subcat) OR maintainence != TRIM (maintainence)

-- DATA STANDARDIZATION & CONSISTENCY

SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintainence
FROM bronze.erp_px_cat_g1v2

------------------------------------ QUALITY CHECK AFTER LOADING ----------------------------------------
------------------------------------ TABLE: silver.erp_px_cat_g1v2 -----------------------------------------

SELECT * FROM silver.erp_px_cat_g1v2  
