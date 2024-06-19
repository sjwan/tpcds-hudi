CREATE TEMPORARY VIEW temp_call_center
USING csv
OPTIONS (path "hdfs://hdp1/tmp/gendata/call_center.dat", header "false", delimiter "|");

INSERT INTO call_center
SELECT 
  CAST(_c0 AS INT) AS cc_call_center_sk, 
  _c1 AS cc_call_center_id, 
  CAST(_c2 AS DATE) AS cc_rec_start_date, 
  CAST(_c3 AS DATE) AS cc_rec_end_date, 
  CAST(_c4 AS INT) AS cc_closed_date_sk, 
  CAST(_c5 AS INT) AS cc_open_date_sk, 
  _c6 AS cc_name, 
  _c7 AS cc_class, 
  CAST(_c8 AS INT) AS cc_employees, 
  CAST(_c9 AS INT) AS cc_sq_ft, 
  _c10 AS cc_hours, 
  _c11 AS cc_manager, 
  CAST(_c12 AS INT) AS cc_mkt_id, 
  _c13 AS cc_mkt_class, 
  _c14 AS cc_mkt_desc, 
  _c15 AS cc_market_manager, 
  CAST(_c16 AS INT) AS cc_division, 
  _c17 AS cc_division_name, 
  CAST(_c18 AS INT) AS cc_company, 
  _c19 AS cc_company_name, 
  _c20 AS cc_street_number, 
  _c21 AS cc_street_name, 
  _c22 AS cc_street_type, 
  _c23 AS cc_suite_number, 
  _c24 AS cc_city, 
  _c25 AS cc_county, 
  _c26 AS cc_state, 
  _c27 AS cc_zip, 
  _c28 AS cc_country, 
  CAST(_c29 AS DECIMAL(5,2)) AS cc_gmt_offset, 
  CAST(_c30 AS DECIMAL(5,2)) AS cc_tax_percentage
FROM temp_call_center;


-- Load customer_address.dat
CREATE TEMPORARY VIEW temp_customer_address
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/customer_address.dat', sep '|', header 'false');

INSERT INTO customer_address
SELECT
  CAST(_c0 AS INT) AS ca_address_sk,
  _c1 AS ca_address_id,
  _c2 AS ca_street_number,
  _c3 AS ca_street_name,
  _c4 AS ca_street_type,
  _c5 AS ca_suite_number,
  _c6 AS ca_city,
  _c7 AS ca_county,
  _c8 AS ca_state,
  _c9 AS ca_zip,
  _c10 AS ca_country,
  CAST(_c11 AS DECIMAL(5,2)) AS ca_gmt_offset,
  _c12 AS ca_location_type
FROM temp_customer_address;


-- Load catalog_page.dat
CREATE TEMPORARY VIEW temp_catalog_page
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/catalog_page.dat', sep '|', header 'false');

-- Insert data into catalog_page
INSERT INTO catalog_page
SELECT
  CAST(_c0 AS INT) AS cp_catalog_page_sk,
  _c1 AS cp_catalog_page_id,
  CAST(coalesce(_c2, 0) AS INT) AS cp_start_date_sk,
  CAST(coalesce(_c3, 0) AS INT) AS cp_end_date_sk,
  _c4 AS cp_department,
  CAST(_c5 AS INT) AS cp_catalog_number,
  CAST(_c6 AS INT) AS cp_catalog_page_number,
  _c7 AS cp_description,
  _c8 AS cp_type
FROM temp_catalog_page;

-- Load catalog_returns.dat
CREATE TEMPORARY VIEW temp_catalog_returns
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/catalog_returns.dat', sep '|', header 'false');

INSERT INTO catalog_returns
SELECT
  CAST(_c0 AS INT) AS cr_returned_date_sk,
  CAST(_c1 AS INT) AS cr_returned_time_sk,
  CAST(_c2 AS INT) AS cr_item_sk,
  CAST(_c3 AS INT) AS cr_refunded_customer_sk,
  CAST(_c4 AS INT) AS cr_refunded_cdemo_sk,
  CAST(_c5 AS INT) AS cr_refunded_hdemo_sk,
  CAST(_c6 AS INT) AS cr_refunded_addr_sk,
  CAST(_c7 AS INT) AS cr_returning_customer_sk,
  CAST(_c8 AS INT) AS cr_returning_cdemo_sk,
  CAST(_c9 AS INT) AS cr_returning_hdemo_sk,
  CAST(_c10 AS INT) AS cr_returning_addr_sk,
  CAST(_c11 AS INT) AS cr_call_center_sk,
  CAST(_c12 AS INT) AS cr_catalog_page_sk,
  CAST(_c13 AS INT) AS cr_ship_mode_sk,
  CAST(_c14 AS INT) AS cr_warehouse_sk,
  CAST(_c15 AS INT) AS cr_reason_sk,
  CAST(_c16 AS INT) AS cr_order_number,
  CAST(_c17 AS INT) AS cr_return_quantity,
  CAST(_c18 AS DECIMAL(7,2)) AS cr_return_amount,
  CAST(_c19 AS DECIMAL(7,2)) AS cr_return_tax,
  CAST(_c20 AS DECIMAL(7,2)) AS cr_return_amt_inc_tax,
  CAST(_c21 AS DECIMAL(7,2)) AS cr_fee,
  CAST(_c22 AS DECIMAL(7,2)) AS cr_return_ship_cost,
  CAST(_c23 AS DECIMAL(7,2)) AS cr_refunded_cash,
  CAST(_c24 AS DECIMAL(7,2)) AS cr_reversed_charge,
  CAST(_c25 AS DECIMAL(7,2)) AS cr_store_credit,
  CAST(_c26 AS DECIMAL(7,2)) AS cr_net_loss
FROM temp_catalog_returns;


-- Load customer_demographics.dat
CREATE TEMPORARY VIEW temp_customer_demographics
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/customer_demographics.dat', sep '|', header 'false');

INSERT INTO customer_demographics
SELECT
  CAST(_c0 AS INT) AS cd_demo_sk,
  _c1 AS cd_gender,
  _c2 AS cd_marital_status,
  _c3 AS cd_education_status,
  CAST(_c4 AS INT) AS cd_purchase_estimate,
  _c5 AS cd_credit_rating,
  CAST(_c6 AS INT) AS cd_dep_count,
  CAST(_c7 AS INT) AS cd_dep_employed_count,
  CAST(_c8 AS INT) AS cd_dep_college_count
FROM temp_customer_demographics;

-- Load customer.dat
CREATE TEMPORARY VIEW temp_customer
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/customer.dat', sep '|', header 'false');

INSERT INTO customer
SELECT
  CAST(_c0 AS INT) AS c_customer_sk,
  _c1 AS c_customer_id,
  CAST(_c2 AS INT) AS c_current_cdemo_sk,
  CAST(_c3 AS INT) AS c_current_hdemo_sk,
  CAST(_c4 AS INT) AS c_current_addr_sk,
  CAST(_c5 AS INT) AS c_first_shipto_date_sk,
  CAST(_c6 AS INT) AS c_first_sales_date_sk,
  _c7 AS c_salutation,
  _c8 AS c_first_name,
  _c9 AS c_last_name,
  _c10 AS c_preferred_cust_flag,
  CAST(_c11 AS INT) AS c_birth_day,
  CAST(_c12 AS INT) AS c_birth_month,
  CAST(_c13 AS INT) AS c_birth_year,
  _c14 AS c_birth_country,
  _c15 AS c_login,
  _c16 AS c_email_address,
  _c17 AS c_last_review_date
FROM temp_customer;

-- Load date_dim.dat
CREATE TEMPORARY VIEW temp_date_dim
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/date_dim.dat', sep '|', header 'false');

INSERT INTO date_dim
SELECT
  CAST(_c0 AS INT) AS d_date_sk,
  _c1 AS d_date_id,
  CAST(_c2 AS DATE) AS d_date,
  CAST(_c3 AS INT) AS d_month_seq,
  CAST(_c4 AS INT) AS d_week_seq,
  CAST(_c5 AS INT) AS d_quarter_seq,
  CAST(_c6 AS INT) AS d_year,
  CAST(_c7 AS INT) AS d_dow,
  CAST(_c8 AS INT) AS d_moy,
  CAST(_c9 AS INT) AS d_dom,
  CAST(_c10 AS INT) AS d_qoy,
  CAST(_c11 AS INT) AS d_fy_year,
  CAST(_c12 AS INT) AS d_fy_quarter_seq,
  CAST(_c13 AS INT) AS d_fy_week_seq,
  _c14 AS d_day_name,
  _c15 AS d_quarter_name,
  _c16 AS d_holiday,
  _c17 AS d_weekend,
  _c18 AS d_following_holiday,
  CAST(_c19 AS INT) AS d_first_dom,
  CAST(_c20 AS INT) AS d_last_dom,
  CAST(_c21 AS INT) AS d_same_day_ly,
  CAST(_c22 AS INT) AS d_same_day_lq,
  _c23 AS d_current_day,
  _c24 AS d_current_week,
  _c25 AS d_current_month,
  _c26 AS d_current_quarter,
  _c27 AS d_current_year
FROM temp_date_dim;

-- Load warehouse.dat
CREATE TEMPORARY VIEW temp_warehouse
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/warehouse.dat', sep '|', header 'false');

INSERT INTO warehouse
SELECT
  CAST(_c0 AS INT) AS w_warehouse_sk,
  _c1 AS w_warehouse_id,
  _c2 AS w_warehouse_name,
  CAST(_c3 AS INT) AS w_warehouse_sq_ft,
  _c4 AS w_street_number,
  _c5 AS w_street_name,
  _c6 AS w_street_type,
  _c7 AS w_suite_number,
  _c8 AS w_city,
  _c9 AS w_county,
  _c10 AS w_state,
  _c11 AS w_zip,
  _c12 AS w_country,
  CAST(_c13 AS DECIMAL(5,2)) AS w_gmt_offset
FROM temp_warehouse;



-- Load ship_mode.dat
CREATE TEMPORARY VIEW temp_ship_mode
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/ship_mode.dat', sep '|', header 'false');

INSERT INTO ship_mode
SELECT
  CAST(_c0 AS INT) AS sm_ship_mode_sk,
  _c1 AS sm_ship_mode_id,
  _c2 AS sm_type,
  _c3 AS sm_code,
  _c4 AS sm_carrier,
  _c5 AS sm_contract
FROM temp_ship_mode;

-- Load time_dim.dat
CREATE TEMPORARY VIEW temp_time_dim
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/time_dim.dat', sep '|', header 'false');

INSERT INTO time_dim
SELECT
  CAST(_c0 AS INT) AS t_time_sk,
  _c1 AS t_time_id,
  CAST(_c2 AS INT) AS t_time,
  CAST(_c3 AS INT) AS t_hour,
  CAST(_c4 AS INT) AS t_minute,
  CAST(_c5 AS INT) AS t_second,
  _c6 AS t_am_pm,
  _c7 AS t_shift,
  _c8 AS t_sub_shift,
  _c9 AS t_meal_time
FROM temp_time_dim;

-- Load reason.dat
CREATE TEMPORARY VIEW temp_reason
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/reason.dat', sep '|', header 'false');

INSERT INTO reason
SELECT
  CAST(_c0 AS INT) AS r_reason_sk,
  _c1 AS r_reason_id,
  _c2 AS r_reason_desc
FROM temp_reason;

-- Load income_band.dat
CREATE TEMPORARY VIEW temp_income_band
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/income_band.dat', sep '|', header 'false');

INSERT INTO income_band
SELECT
  CAST(_c0 AS INT) AS ib_income_band_sk,
  CAST(_c1 AS INT) AS ib_lower_bound,
  CAST(_c2 AS INT) AS ib_upper_bound
FROM temp_income_band;

-- Load item.dat
CREATE TEMPORARY VIEW temp_item
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/item.dat', sep '|', header 'false');

INSERT INTO item
SELECT
  CAST(_c0 AS INT) AS i_item_sk,
  _c1 AS i_item_id,
  CAST(_c2 AS DATE) AS i_rec_start_date,
  CAST(_c3 AS DATE) AS i_rec_end_date,
  _c4 AS i_item_desc,
  CAST(_c5 AS DECIMAL(7,2)) AS i_current_price,
  CAST(_c6 AS DECIMAL(7,2)) AS i_wholesale_cost,
  CAST(_c7 AS INT) AS i_brand_id,
  _c8 AS i_brand,
  CAST(_c9 AS INT) AS i_class_id,
  _c10 AS i_class,
  CAST(_c11 AS INT) AS i_category_id,
  _c12 AS i_category,
  CAST(_c13 AS INT) AS i_manufact_id,
  _c14 AS i_manufact,
  _c15 AS i_size,
  _c16 AS i_formulation,
  _c17 AS i_color,
  _c18 AS i_units,
  _c19 AS i_container,
  CAST(_c20 AS INT) AS i_manager_id,
  _c21 AS i_product_name
FROM temp_item;

-- Load store.dat
CREATE TEMPORARY VIEW temp_store
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/store.dat', sep '|', header 'false');

INSERT INTO store
SELECT
  CAST(_c0 AS INT) AS s_store_sk,
  _c1 AS s_store_id,
  CAST(_c2 AS DATE) AS s_rec_start_date,
  CAST(_c3 AS DATE) AS s_rec_end_date,
  CAST(_c4 AS INT) AS s_closed_date_sk,
  _c5 AS s_store_name,
  CAST(_c6 AS INT) AS s_number_employees,
  CAST(_c7 AS INT) AS s_floor_space,
  _c8 AS s_hours,
  _c9 AS s_manager,
  CAST(_c10 AS INT) AS s_market_id,
  _c11 AS s_geography_class,
  _c12 AS s_market_desc,
  _c13 AS s_market_manager,
  CAST(_c14 AS INT) AS s_division_id,
  _c15 AS s_division_name,
  CAST(_c16 AS INT) AS s_company_id,
  _c17 AS s_company_name,
  _c18 AS s_street_number,
  _c19 AS s_street_name,
  _c20 AS s_street_type,
  _c21 AS s_suite_number,
  _c22 AS s_city,
  _c23 AS s_county,
  _c24 AS s_state,
  _c25 AS s_zip,
  _c26 AS s_country,
  CAST(_c27 AS DECIMAL(5,2)) AS s_gmt_offset,
  CAST(_c28 AS DECIMAL(5,2)) AS s_tax_precentage
FROM temp_store;

-- Load store_returns.dat
CREATE TEMPORARY VIEW temp_store_returns
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/store_returns.dat', sep '|', header 'false');

INSERT INTO store_returns
SELECT
  CAST(coalesce(_c0,0) AS INT) AS sr_returned_date_sk,
  CAST(_c1 AS INT) AS sr_return_time_sk,
  CAST(_c2 AS INT) AS sr_item_sk,
  CAST(_c3 AS INT) AS sr_customer_sk,
  CAST(_c4 AS INT) AS sr_cdemo_sk,
  CAST(_c5 AS INT) AS sr_hdemo_sk,
  CAST(_c6 AS INT) AS sr_addr_sk,
  CAST(_c7 AS INT) AS sr_store_sk,
  CAST(_c8 AS INT) AS sr_reason_sk,
  CAST(_c9 AS INT) AS sr_ticket_number,
  CAST(_c10 AS INT) AS sr_return_quantity,
  CAST(_c11 AS DECIMAL(7,2)) AS sr_return_amt,
  CAST(_c12 AS DECIMAL(7,2)) AS sr_return_tax,
  CAST(_c13 AS DECIMAL(7,2)) AS sr_return_amt_inc_tax,
  CAST(_c14 AS DECIMAL(7,2)) AS sr_fee,
  CAST(_c15 AS DECIMAL(7,2)) AS sr_return_ship_cost,
  CAST(_c16 AS DECIMAL(7,2)) AS sr_refunded_cash,
  CAST(coalesce(_c17,0) AS DECIMAL(7,2)) AS sr_reversed_charge,
  CAST(coalesce(_c18,0) AS DECIMAL(7,2)) AS sr_store_credit,
  CAST(coalesce(_c19,0) AS DECIMAL(7,2)) AS sr_net_loss
FROM temp_store_returns;

-- Load household_demographics.dat
CREATE TEMPORARY VIEW temp_household_demographics
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/household_demographics.dat', sep '|', header 'false');

INSERT INTO household_demographics
SELECT
  CAST(_c0 AS INT) AS hd_demo_sk,
  CAST(_c1 AS INT) AS hd_income_band_sk,
  _c2 AS hd_buy_potential,
  CAST(_c3 AS INT) AS hd_dep_count,
  CAST(_c4 AS INT) AS hd_vehicle_count
FROM temp_household_demographics;

-- Load web_page.dat
CREATE TEMPORARY VIEW temp_web_page
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/web_page.dat', sep '|', header 'false');

INSERT INTO web_page
SELECT
  CAST(_c0 AS INT) AS wp_web_page_sk,
  _c1 AS wp_web_page_id,
  CAST(_c2 AS DATE) AS wp_rec_start_date,
  CAST(_c3 AS DATE) AS wp_rec_end_date,
  CAST(_c4 AS INT) AS wp_creation_date_sk,
  CAST(_c5 AS INT) AS wp_access_date_sk,
  _c6 AS wp_autogen_flag,
  CAST(_c7 AS INT) AS wp_customer_sk,
  _c8 AS wp_url,
  _c9 AS wp_type,
  CAST(_c10 AS INT) AS wp_char_count,
  CAST(_c11 AS INT) AS wp_link_count,
  CAST(_c12 AS INT) AS wp_image_count,
  CAST(_c13 AS INT) AS wp_max_ad_count
FROM temp_web_page;
-- Load promotion.dat
CREATE TEMPORARY VIEW temp_promotion
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/promotion.dat', sep '|', header 'false');

INSERT INTO promotion
SELECT
  CAST(_c0 AS INT) AS p_promo_sk,
  _c1 AS p_promo_id,
  CAST(coalesce(_c2,0) AS INT) AS p_start_date_sk,
  CAST(coalesce(_c3,0) AS INT) AS p_end_date_sk,
  CAST(coalesce(_c4,0) AS INT) AS p_item_sk,
  CAST(_c5 AS DECIMAL(15,2)) AS p_cost,
  CAST(coalesce(_c6,0) AS INT) AS p_response_target,
  _c7 AS p_promo_name,
  _c8 AS p_channel_dmail,
  _c9 AS p_channel_email,
  _c10 AS p_channel_catalog,
  _c11 AS p_channel_tv,
  _c12 AS p_channel_radio,
  _c13 AS p_channel_press,
  _c14 AS p_channel_event,
  _c15 AS p_channel_demo,
  _c16 AS p_channel_details,
  _c17 AS p_purpose,
  _c18 AS p_discount_active
FROM temp_promotion;
-- Load inventory.dat
CREATE TEMPORARY VIEW temp_inventory
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/inventory.dat', sep '|', header 'false');

INSERT INTO inventory
SELECT
  CAST(_c0 AS INT) AS inv_date_sk,
  CAST(_c1 AS INT) AS inv_item_sk,
  CAST(_c2 AS INT) AS inv_warehouse_sk,
  CAST(_c3 AS INT) AS inv_quantity_on_hand
FROM temp_inventory;

-- Load web_returns.dat
CREATE TEMPORARY VIEW temp_web_returns
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/web_returns.dat', sep '|', header 'false');

INSERT INTO web_returns
SELECT
  CAST(coalesce(_c0, 0) AS INT) AS wr_returned_date_sk,
  CAST(coalesce(_c1, 0) AS INT) AS wr_returned_time_sk,
  CAST(_c2 AS INT) AS wr_item_sk,
  CAST(_c3 AS INT) AS wr_refunded_customer_sk,
  CAST(coalesce(_c4, 0) AS INT) AS wr_refunded_cdemo_sk,
  CAST(_c5 AS INT) AS wr_refunded_hdemo_sk,
  CAST(_c6 AS INT) AS wr_refunded_addr_sk,
  CAST(_c7 AS INT) AS wr_returning_customer_sk,
  CAST(_c8 AS INT) AS wr_returning_cdemo_sk,
  CAST(_c9 AS INT) AS wr_returning_hdemo_sk,
  CAST(_c10 AS INT) AS wr_returning_addr_sk,
  CAST(_c11 AS INT) AS wr_web_page_sk,
  CAST(_c12 AS INT) AS wr_reason_sk,
  CAST(_c13 AS INT) AS wr_order_number,
  CAST(_c14 AS INT) AS wr_return_quantity,
  CAST(coalesce(_c15, 0) AS DECIMAL(7,2)) AS wr_return_amt,
  CAST(coalesce(_c16, 0) AS DECIMAL(7,2)) AS wr_return_tax,
  CAST(coalesce(_c17, 0) AS DECIMAL(7,2)) AS wr_return_amt_inc_tax,
  CAST(coalesce(_c18, 0) AS DECIMAL(7,2)) AS wr_fee,
  CAST(coalesce(_c19, 0) AS DECIMAL(7,2)) AS wr_return_ship_cost,
  CAST(coalesce(_c20,0) AS DECIMAL(7,2)) AS wr_refunded_cash,
  CAST(coalesce(_c21,0) AS DECIMAL(7,2)) AS wr_reversed_charge,
  CAST(coalesce(_c22,0) AS DECIMAL(7,2)) AS wr_account_credit,
  CAST(coalesce(_c23,0) AS DECIMAL(7,2)) AS wr_net_loss
FROM temp_web_returns;

-- Load web_site.dat
CREATE TEMPORARY VIEW temp_web_site
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/web_site.dat', sep '|', header 'false');

INSERT INTO web_site
SELECT
  CAST(_c0 AS INT) AS web_site_sk,
  _c1 AS web_site_id,
  CAST(_c2 AS DATE) AS web_rec_start_date,
  CAST(_c3 AS DATE) AS web_rec_end_date,
  _c4 AS web_name,
  CAST(_c5 AS INT) AS web_open_date_sk,
  CAST(_c6 AS INT) AS web_close_date_sk,
  _c7 AS web_class,
  _c8 AS web_manager,
  CAST(_c9 AS INT) AS web_mkt_id,
  _c10 AS web_mkt_class,
  _c11 AS web_mkt_desc,
  _c12 AS web_market_manager,
  CAST(_c13 AS INT) AS web_company_id,
  _c14 AS web_company_name,
  _c15 AS web_street_number,
  _c16 AS web_street_name,
  _c17 AS web_street_type,
  _c18 AS web_suite_number,
  _c19 AS web_city,
  _c20 AS web_county,
  _c21 AS web_state,
  _c22 AS web_zip,
  _c23 AS web_country,
  CAST(_c24 AS DECIMAL(5,2)) AS web_gmt_offset,
  CAST(_c25 AS DECIMAL(5,2)) AS web_tax_percentage
FROM temp_web_site;

-- Load web_sales.dat
CREATE TEMPORARY VIEW temp_web_sales
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/web_sales.dat', sep '|', header 'false');

INSERT INTO web_sales
SELECT
  CAST(coalesce(_c0 , 0) AS INT) AS ws_sold_date_sk,
  CAST(coalesce(_c1 , 0) AS INT) AS ws_sold_time_sk,
  CAST(coalesce(_c2 , 0) AS INT) AS ws_ship_date_sk,
  CAST(coalesce(_c3 , 0) AS INT) AS ws_item_sk,
  CAST(coalesce(_c4 , 0) AS INT) AS ws_bill_customer_sk,
  CAST(coalesce(_c5 , 0) AS INT) AS ws_bill_cdemo_sk,
  CAST(coalesce(_c6 , 0) AS INT) AS ws_bill_hdemo_sk,
  CAST(coalesce(_c7 , 0) AS INT) AS ws_bill_addr_sk,
  CAST(coalesce(_c8 , 0) AS INT) AS ws_ship_customer_sk,
  CAST(coalesce(_c9 , 0) AS INT) AS ws_ship_cdemo_sk,
  CAST(coalesce(_c10, 0)  AS INT) AS ws_ship_hdemo_sk,
  CAST(coalesce(_c11, 0)  AS INT) AS ws_ship_addr_sk,
  CAST(coalesce(_c12, 0)  AS INT) AS ws_web_page_sk,
  CAST(coalesce(_c13, 0)  AS INT) AS ws_web_site_sk,
  CAST(coalesce(_c14, 0)  AS INT) AS ws_ship_mode_sk,
  CAST(coalesce(_c15, 0)  AS INT) AS ws_warehouse_sk,
  CAST(coalesce(_c16, 0)  AS INT) AS ws_promo_sk,
  CAST(coalesce(_c17, 0)  AS INT) AS ws_order_number,
  CAST(coalesce(_c18, 0)  AS INT) AS ws_quantity,
  CAST(coalesce(_c19, 0)  AS DECIMAL(7,2)) AS ws_wholesale_cost,
  CAST(coalesce(_c20, 0)  AS DECIMAL(7,2)) AS ws_list_price,
  CAST(coalesce(_c21, 0)  AS DECIMAL(7,2)) AS ws_sales_price,
  CAST(coalesce(_c22, 0)  AS DECIMAL(7,2)) AS ws_ext_discount_amt,
  CAST(coalesce(_c23, 0)  AS DECIMAL(7,2)) AS ws_ext_sales_price,
  CAST(coalesce(_c24, 0)  AS DECIMAL(7,2)) AS ws_ext_wholesale_cost,
  CAST(coalesce(_c25, 0)  AS DECIMAL(7,2)) AS ws_ext_list_price,
  CAST(coalesce(_c26, 0)  AS DECIMAL(7,2)) AS ws_ext_tax,
  CAST(coalesce(_c27, 0)  AS DECIMAL(7,2)) AS ws_coupon_amt,
  CAST(coalesce(_c28, 0)  AS DECIMAL(7,2)) AS ws_ext_ship_cost,
  CAST(coalesce(_c29, 0)  AS DECIMAL(7,2)) AS ws_net_paid,
  CAST(coalesce(_c30, 0)  AS DECIMAL(7,2)) AS ws_net_paid_inc_tax,
  CAST(coalesce(_c31, 0)  AS DECIMAL(7,2)) AS ws_net_paid_inc_ship,
  CAST(coalesce(_c32, 0)  AS DECIMAL(7,2)) AS ws_net_paid_inc_ship_tax,
  CAST(coalesce(_c33, 0)  AS DECIMAL(7,2)) AS ws_net_profit
FROM temp_web_sales;

-- Load catalog_sales.dat
CREATE TEMPORARY VIEW temp_catalog_sales
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/catalog_sales.dat', sep '|', header 'false');

INSERT INTO catalog_sales
SELECT
  CAST(coalesce(_c0 ,0) AS INT) AS cs_sold_date_sk,
  CAST(coalesce(_c1 ,0) AS INT) AS cs_sold_time_sk,
  CAST(coalesce(_c2 ,0) AS INT) AS cs_ship_date_sk,
  CAST(coalesce(_c3 ,0) AS INT) AS cs_bill_customer_sk,
  CAST(coalesce(_c4 ,0) AS INT) AS cs_bill_cdemo_sk,
  CAST(coalesce(_c5 ,0) AS INT) AS cs_bill_hdemo_sk,
  CAST(coalesce(_c6 ,0) AS INT) AS cs_bill_addr_sk,
  CAST(coalesce(_c7 ,0) AS INT) AS cs_ship_customer_sk,
  CAST(coalesce(_c8 ,0) AS INT) AS cs_ship_cdemo_sk,
  CAST(coalesce(_c9 ,0) AS INT) AS cs_ship_hdemo_sk,
  CAST(coalesce(_c10,0)  AS INT) AS cs_ship_addr_sk,
  CAST(coalesce(_c11,0)  AS INT) AS cs_call_center_sk,
  CAST(coalesce(_c12,0)  AS INT) AS cs_catalog_page_sk,
  CAST(coalesce(_c13,0)  AS INT) AS cs_ship_mode_sk,
  CAST(coalesce(_c14,0)  AS INT) AS cs_warehouse_sk,
  CAST(coalesce(_c15,0)  AS INT) AS cs_item_sk,
  CAST(coalesce(_c16,0)  AS INT) AS cs_promo_sk,
  CAST(coalesce(_c17,0)  AS INT) AS cs_order_number,
  CAST(coalesce(_c18,0)  AS INT) AS cs_quantity,
  CAST(coalesce(_c19,0)  AS DECIMAL(7,2)) AS cs_wholesale_cost,
  CAST(coalesce(_c20,0)  AS DECIMAL(7,2)) AS cs_list_price,
  CAST(coalesce(_c21,0)  AS DECIMAL(7,2)) AS cs_sales_price,
  CAST(coalesce(_c22,0)  AS DECIMAL(7,2)) AS cs_ext_discount_amt,
  CAST(coalesce(_c23,0)  AS DECIMAL(7,2)) AS cs_ext_sales_price,
  CAST(coalesce(_c24,0)  AS DECIMAL(7,2)) AS cs_ext_wholesale_cost,
  CAST(coalesce(_c25,0)  AS DECIMAL(7,2)) AS cs_ext_list_price,
  CAST(coalesce(_c26,0)  AS DECIMAL(7,2)) AS cs_ext_tax,
  CAST(coalesce(_c27,0)  AS DECIMAL(7,2)) AS cs_coupon_amt,
  CAST(coalesce(_c28,0)  AS DECIMAL(7,2)) AS cs_ext_ship_cost,
  CAST(coalesce(_c29,0)  AS DECIMAL(7,2)) AS cs_net_paid,
  CAST(coalesce(_c30,0)  AS DECIMAL(7,2)) AS cs_net_paid_inc_tax,
  CAST(coalesce(_c31,0)  AS DECIMAL(7,2)) AS cs_net_paid_inc_ship,
  CAST(coalesce(_c32,0)  AS DECIMAL(7,2)) AS cs_net_paid_inc_ship_tax,
  CAST(coalesce(_c33,0)  AS DECIMAL(7,2)) AS cs_net_profit
FROM temp_catalog_sales;

-- Load store_sales.dat
CREATE TEMPORARY VIEW temp_store_sales
USING csv
OPTIONS (path 'hdfs://hdp1/tmp/gendata/store_sales.dat', sep '|', header 'false');

INSERT INTO store_sales
SELECT
  CAST(_c0 AS INT) AS ss_sold_date_sk,
  CAST(_c1 AS INT) AS ss_sold_time_sk,
  CAST(_c2 AS INT) AS ss_item_sk,
  CAST(_c3 AS INT) AS ss_customer_sk,
  CAST(_c4 AS INT) AS ss_cdemo_sk,
  CAST(_c5 AS INT) AS ss_hdemo_sk,
  CAST(_c6 AS INT) AS ss_addr_sk,
  CAST(_c7 AS INT) AS ss_store_sk,
  CAST(_c8 AS INT) AS ss_promo_sk,
  CAST(_c9 AS INT) AS ss_ticket_number,
  CAST(_c10 AS INT) AS ss_quantity,
  CAST(_c11 AS DECIMAL(7,2)) AS ss_wholesale_cost,
  CAST(_c12 AS DECIMAL(7,2)) AS ss_list_price,
  CAST(_c13 AS DECIMAL(7,2)) AS ss_sales_price,
  CAST(_c14 AS DECIMAL(7,2)) AS ss_ext_discount_amt,
  CAST(_c15 AS DECIMAL(7,2)) AS ss_ext_sales_price,
  CAST(_c16 AS DECIMAL(7,2)) AS ss_ext_wholesale_cost,
  CAST(_c17 AS DECIMAL(7,2)) AS ss_ext_list_price,
  CAST(_c18 AS DECIMAL(7,2)) AS ss_ext_tax,
  CAST(_c19 AS DECIMAL(7,2)) AS ss_coupon_amt,
  CAST(_c20 AS DECIMAL(7,2)) AS ss_net_paid,
  CAST(_c21 AS DECIMAL(7,2)) AS ss_net_paid_inc_tax,
  CAST(_c22 AS DECIMAL(7,2)) AS ss_net_profit
FROM temp_store_sales;