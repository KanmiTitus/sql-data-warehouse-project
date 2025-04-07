
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose: 
      This script create views for gold layer in the data warehouse
	  The Gold layer represent the final dimension and facts table (Star Schema)

	  Each views performs transformation and combines data from the Silver layer
	  to produce a clean, enriched, and business reporting ready.

Usage: 
	This views can be queried directly for analytics and reporting
==============================================================================
*/

 -- ==========================================================================
-- Create Dimension: gold.dim_customers
--  ==========================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL 
   DROP VIEW gold.dim_customers;

   GO

CREATE VIEW gold.dim_customers AS 

  (
	SELECT 
	     ROW_NUMBER() OVER (ORDER BY ci.cust_key) AS customer_key,
		 ci.cust_id AS customer_id,
		 ci.cust_key AS customer_number,
		 ci.cust_firstname AS first_name,
		 ci.cust_lastname AS last_name,
		 ci.cust_marital_status AS marital_status,
		 la.country AS country,
		 CASE WHEN ci.cust_gender != 'n/a' THEN ci.cust_gender -- CRM is master source for gender accuracy
			 ELSE COALESCE(ca.gender, 'n/a')
			 END AS gender,
		 ca.birthdate AS birthdate,
		 ci.cust_create_date AS create_date

	FROM silver.crm_cust_info ci      -- Alias 'ci'
	LEFT JOIN silver.erp_cust_az12 ca -- Alias 'ca'
	   ON     ci.cust_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la  --Alias 'la'
	   ON ci.cust_key = la.cid
 )

     

-- ===========================================================================
-- Create Dimension: gold.dim_products
-- ===========================================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL 
   DROP VIEW gold.dim_products;

   GO

CREATE VIEW gold.dim_products AS 
  
  (
	SELECT 
	     ROW_NUMBER() OVER( ORDER BY pi.prd_start_date, pi.prd_key) AS product_key,
		 pi.prd_id AS product_id,
		 pi.prd_key AS product_number,
		 pi.prd_name AS product_name,
		 pi.cat_id AS category_id,
		 cg.cat AS category,
		 cg.subcat AS sub_category,
		 cg.maintenance AS maintenance_status,
		 pi.prd_cost AS cost,
		 pi.prd_line AS product_line,
		 pi.prd_start_date AS start_date
		 
		
		   FROM silver.crm_prd_info pi
		   LEFT JOIN silver.erp_px_cat_g1v2 cg 
			  ON pi.cat_id = cg.id
		   WHERE prd_end_date IS NULL -- Filtering out historical data

		   )
		

-- ==========================================================================
-- Create Dimension: gold.fact_sales
-- ===========================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL 
   DROP VIEW gold.fact_sales;

   GO

 CREATE VIEW gold.fact_sales AS

 (
	SELECT 
		 
		 sls_ord_num AS order_number,
		 cu.customer_key AS customer_key,
		 pr.product_key AS product_key,
		 sls_order_dt AS order_date,
		 sls_ship_dt AS ship_date,
		 sls_due_dt AS due_date,
		 sls_quantity AS quantity,
		 sls_sales AS sales,
		 sls_price AS price
	
		FROM silver.crm_sales_details sd
		LEFT JOIN gold.dim_customers cu
		  ON sd.sls_cust_id = cu.customer_id
		LEFT JOIN gold.dim_products pr
		  ON sd.sls_prd_key = pr.product_number

		)
