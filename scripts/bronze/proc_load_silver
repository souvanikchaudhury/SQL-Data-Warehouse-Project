CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
    Truncate table silver.crm_cust_info
    Insert into silver.crm_cust_info (
	    cst_id,
	    cst_key,
	    cst_firstname,
	    cst_lastname,
	    cst_marital_status,
	    cst_gndr,
	    cst_create_date
	
    )

    Select 
    cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname ,
    trim(cst_lastname) as cst_lastname,
    CASE WHEN UPPER(Trim(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(Trim(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'n/a'
    END cst_gndr,
    CASE WHEN UPPER(Trim(cst_material_status)) = 'S' THEN 'Single'
    WHEN UPPER(Trim(cst_material_status)) = 'M' THEN 'Married'
    ELSE 'n/a'
    END cst_material_status,
    cst_create_date
    from(
	    SELECT
	    *,
	    ROW_NUMBER () OVER (PARTITION BY cst_id order by cst_create_date DESC) as flag_last
	    FROM bronze.crm_cust_info
	    Where cst_id is not null 
    ) t
    Where flag_last = 1




    Truncate table silver.crm_prd_info
    Insert into silver.crm_prd_info (
        prd_id ,   
        cat_id,
        prd_key ,
        prd_nm   ,    
        prd_cost ,    
        prd_line    , 
        prd_start_dt ,
        prd_end_dt     
    )
    select
    prd_id,
    Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,len(prd_key))as prd_key,
    prd_nm,
    isnull(prd_cost,0) as prd_cost,
    case upper(Trim(prd_line))
	    When 'M' Then 'Mountain'
	    When 'R' Then 'Road'
	    When 'S' Then 'Other Sales'
	    When 'T' Then 'Touring'
	    else 'n/a'
    End as prd_line,
    Cast(prd_start_dt as DATE) as prd_start_date,
    Cast(Lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as DATE) as prd_end_dt

    FROM BRONZE.crm_prd_info



    Truncate table silver.crm_sales_details
    Insert into silver.crm_sales_details (

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
    Select
        sls_ord_num  ,
        sls_prd_key  ,
        sls_cust_id  ,
        Case When  sls_order_dt = 0 or len(sls_order_dt) != 8 Then Null
        else cast(cast(sls_order_dt as VARCHAR) as DATE)
        end as sls_order_dt,
        Case When  sls_ship_dt = 0 or len(sls_ship_dt) != 8 Then Null
        else cast(cast(sls_ship_dt as VARCHAR) as DATE)
        end as sls_ship_dt,
        Case When  sls_due_dt = 0 or len(sls_due_dt) != 8 Then Null
        else cast(cast(sls_due_dt as VARCHAR) as DATE)
        end as sls_due_dt,
        CASE WHen sls_sales is null or sls_sales <= 0 or sls_sales != abs(sls_price) * sls_quantity
                Then sls_quantity * abs(sls_price)
             else sls_sales
        end as sls_sales,
        sls_quantity,
        CASE When sls_price is null or sls_price <= 0 
                Then sls_sales/nullif(sls_quantity,0)
             else sls_price
        end as sls_price
    from bronze.crm_sales_details





    Truncate table silver.erp_cust_az12
    Insert into silver.erp_cust_az12 (
    cid,
    bdate,
    gen
    )
    SELECT
    Case When cid like 'NAS%' then substring(cid,4,len(cid))
    else cid
    end cid,
    Case When bdate > getdate() then null
    else bdate
    end bdate,
    Case When upper(trim(gen)) in ('F','Female') then 'Female'
	     When upper(trim(gen)) in ('M','Male') then 'Male'
	     else 'n/a'
    End as gen
    from bronze.erp_cust_az12


    Truncate table silver.erp_loc_a101
    Insert into silver.erp_loc_a101(
    cid,
    cntry
    )
    Select
    Replace(cid,'-','') as cid,
    Case When trim(cntry) = 'DE' then 'Germany'
	    When trim(cntry) In ('USA','US') then 'United States' 
	    When trim(cntry)  = '' or cntry is NULL then 'n/a'
	    else trim(cntry)
    end as cntry
    from bronze.erp_loc_a101





    Truncate table silver.erp_px_cat_g1v2
    Insert into silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance
    )

    Select
    id,
    cat,
    subcat,
    maintenance

    from bronze.erp_px_cat_g1v2
end
