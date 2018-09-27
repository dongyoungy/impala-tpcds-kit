use tpcds_5000_parquet;

create table if not exists cs_st_1_100 like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table cs_st_1_100
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(select *, row_number() over (partition by w_state, i_item_id order by w_state, i_item_id) as rownum, count(*) over (partition by w_state, i_item_id) as groupsize
from catalog_sales, item, warehouse where cs_warehouse_sk = w_warehouse_sk and cs_item_sk = i_item_sk order by rand()) tmp
where tmp.rownum <= 100 or (tmp.rownum > 100 and rand(unix_timestamp()) < (100 / 10) / tmp.groupsize);

create table if not exists cs_st_1_1000 like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table cs_st_1_1000
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(select *, row_number() over (partition by w_state, i_item_id order by w_state, i_item_id) as rownum, count(*) over (partition by w_state, i_item_id) as groupsize
from catalog_sales, item, warehouse where cs_warehouse_sk = w_warehouse_sk and cs_item_sk = i_item_sk order by rand()) tmp
where tmp.rownum <= 1000 or (tmp.rownum > 1000 and rand(unix_timestamp()) < (1000 / 10) / tmp.groupsize);

create table if not exists ss_st_3_100 like tpcds_5000_text.store_sales stored as parquet;
insert overwrite table ss_st_3_100
select
ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit,c_customer_sk,c_customer_id,c_current_cdemo_sk,c_current_hdemo_sk,c_current_addr_sk,c_first_shipto_date_sk,c_first_sales_date_sk,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address,c_last_review_date,i_item_sk,i_item_id,i_rec_start_date,i_rec_end_date,i_item_desc,i_current_price,i_wholesale_cost,i_brand_id,i_brand,i_class_id,i_class,i_category_id,i_category,i_manufact_id,i_manufact,i_size,i_formulation,i_color,i_units,i_container,i_manager_id,i_product_name,s_store_sk,s_store_id,s_rec_start_date,s_rec_end_date,s_closed_date_sk,s_store_name,s_number_employees,s_floor_space,s_hours,s_manager,s_market_id,s_geography_class,s_market_desc,s_market_manager,s_division_id,s_division_name,s_company_id,s_company_name,s_street_number,s_street_name,s_street_type,s_suite_number,s_city,s_county,s_state,s_zip,s_country,s_gmt_offset,s_tax_precentage,rand(unix_timestamp()) as v_rand
from
(select *, row_number() over (partition by ss_store_sk order by ss_store_sk) as rownum, count(*) over (partition by ss_store_sk) as groupsize
from store_sales order by rand()) tmp
where tmp.rownum <= 100 or (tmp.rownum > 100 and rand(unix_timestamp()) < (100 / 10) / tmp.groupsize);

create table if not exists ss_st_3_1000 like tpcds_5000_text.store_sales stored as parquet;
insert overwrite table ss_st_3_1000
select
ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit,c_customer_sk,c_customer_id,c_current_cdemo_sk,c_current_hdemo_sk,c_current_addr_sk,c_first_shipto_date_sk,c_first_sales_date_sk,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address,c_last_review_date,i_item_sk,i_item_id,i_rec_start_date,i_rec_end_date,i_item_desc,i_current_price,i_wholesale_cost,i_brand_id,i_brand,i_class_id,i_class,i_category_id,i_category,i_manufact_id,i_manufact,i_size,i_formulation,i_color,i_units,i_container,i_manager_id,i_product_name,s_store_sk,s_store_id,s_rec_start_date,s_rec_end_date,s_closed_date_sk,s_store_name,s_number_employees,s_floor_space,s_hours,s_manager,s_market_id,s_geography_class,s_market_desc,s_market_manager,s_division_id,s_division_name,s_company_id,s_company_name,s_street_number,s_street_name,s_street_type,s_suite_number,s_city,s_county,s_state,s_zip,s_country,s_gmt_offset,s_tax_precentage,rand(unix_timestamp()) as v_rand
from
(select *, row_number() over (partition by ss_store_sk order by ss_store_sk) as rownum, count(*) over (partition by ss_store_sk) as groupsize
from store_sales order by rand()) tmp
where tmp.rownum <= 1000 or (tmp.rownum > 1000 and rand(unix_timestamp()) < (1000 / 10) / tmp.groupsize);

create table if not exists cs_uv_1_1p like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite cs_uv_1_1p
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(
select * from catalog_sales where abs(fnv_hash(concat_ws(',', cast(cs_item_sk as string), cast(cs_order_number as string)))) % 100000000 < (100000000 / 100)
) tmp;

create table if not exists cs_uv_1_10p like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite cs_uv_1_10p
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(
select * from catalog_sales where abs(fnv_hash(concat_ws(',', cast(cs_item_sk as string), cast(cs_order_number as string)))) % 100000000 < (100000000 / 10)
) tmp;

create table if not exists cr_uv_1_1p like tpcds_5000_text.catalog_returns stored as parquet;
insert overwrite cr_uv_1_1p
select
cr_returned_date_sk,cr_returned_time_sk,cr_item_sk,cr_refunded_customer_sk,cr_refunded_cdemo_sk,cr_refunded_hdemo_sk,cr_refunded_addr_sk,cr_returning_customer_sk,cr_returning_cdemo_sk,cr_returning_hdemo_sk,cr_returning_addr_sk,cr_call_center_sk,cr_catalog_page_sk,cr_ship_mode_sk,cr_warehouse_sk,cr_reason_sk,cr_order_number,cr_return_quantity,cr_return_amount,cr_return_tax,cr_return_amt_inc_tax,cr_fee,cr_return_ship_cost,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss
from
(
select * from catalog_returns where abs(fnv_hash(concat_ws(',', cast(cr_item_sk as string), cast(cr_order_number as string)))) % 100000000 < (100000000 / 100)
) tmp;

create table if not exists cr_uv_1_10p like tpcds_5000_text.catalog_returns stored as parquet;
insert overwrite cr_uv_1_10p
select
cr_returned_date_sk,cr_returned_time_sk,cr_item_sk,cr_refunded_customer_sk,cr_refunded_cdemo_sk,cr_refunded_hdemo_sk,cr_refunded_addr_sk,cr_returning_customer_sk,cr_returning_cdemo_sk,cr_returning_hdemo_sk,cr_returning_addr_sk,cr_call_center_sk,cr_catalog_page_sk,cr_ship_mode_sk,cr_warehouse_sk,cr_reason_sk,cr_order_number,cr_return_quantity,cr_return_amount,cr_return_tax,cr_return_amt_inc_tax,cr_fee,cr_return_ship_cost,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss
from
(
select * from catalog_returns where abs(fnv_hash(concat_ws(',', cast(cr_item_sk as string), cast(cr_order_number as string)))) % 100000000 < (100000000 / 10)
) tmp;

create table if not exists ws_uv_1_1p like tpcds_5000_text.web_sales stored as parquet;
insert overwrite ws_uv_1_1p
select
ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit
from
(
select * from web_sales where abs(fnv_hash(concat_ws(',', cast(ws_item_sk as string), cast(ws_order_number as string)))) % 100000000 < (100000000 / 100)
) tmp;

create table if not exists ws_uv_1_10p like tpcds_5000_text.web_sales stored as parquet;
insert overwrite ws_uv_1_10p
select
ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit
from
(
select * from web_sales where abs(fnv_hash(concat_ws(',', cast(ws_item_sk as string), cast(ws_order_number as string)))) % 100000000 < (100000000 / 10)
) tmp;

create table if not exists wr_uv_1_1p like tpcds_5000_text.web_returns stored as parquet;
insert overwrite wr_uv_1_1p
select
wr_returned_date_sk,wr_returned_time_sk,wr_item_sk,wr_refunded_customer_sk,wr_refunded_cdemo_sk,wr_refunded_hdemo_sk,wr_refunded_addr_sk,wr_returning_customer_sk,wr_returning_cdemo_sk,wr_returning_hdemo_sk,wr_returning_addr_sk,wr_web_page_sk,wr_reason_sk,wr_order_number,wr_return_quantity,wr_return_amt,wr_return_tax,wr_return_amt_inc_tax,wr_fee,wr_return_ship_cost,wr_refunded_cash,wr_reversed_charge,wr_account_credit,wr_net_loss
from
(
select * from web_returns where abs(fnv_hash(concat_ws(',', cast(wr_item_sk as string), cast(wr_order_number as string)))) % 100000000 < (100000000 / 100)
) tmp;

create table if not exists wr_uv_1_10p like tpcds_5000_text.web_returns stored as parquet;
insert overwrite wr_uv_1_10p
select
wr_returned_date_sk,wr_returned_time_sk,wr_item_sk,wr_refunded_customer_sk,wr_refunded_cdemo_sk,wr_refunded_hdemo_sk,wr_refunded_addr_sk,wr_returning_customer_sk,wr_returning_cdemo_sk,wr_returning_hdemo_sk,wr_returning_addr_sk,wr_web_page_sk,wr_reason_sk,wr_order_number,wr_return_quantity,wr_return_amt,wr_return_tax,wr_return_amt_inc_tax,wr_fee,wr_return_ship_cost,wr_refunded_cash,wr_reversed_charge,wr_account_credit,wr_net_loss
from
(
select * from web_returns where abs(fnv_hash(concat_ws(',', cast(wr_item_sk as string), cast(wr_order_number as string)))) % 100000000 < (100000000 / 10)
) tmp;

create table if not exists cs_st_2_100 like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table cs_st_2_100
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(select *, row_number() over (partition by cs_catalog_page_sk order by cs_catalog_page_sk) as rownum, count(*) over (partition by cs_catalog_page_sk) as groupsize
from catalog_sales order by rand()) tmp
where tmp.rownum <= 100 or (tmp.rownum > 100 and rand(unix_timestamp()) < (100 / 10) / tmp.groupsize);

create table if not exists cs_st_2_1000 like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table cs_st_2_1000
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(select *, row_number() over (partition by cs_catalog_page_sk order by cs_catalog_page_sk) as rownum, count(*) over (partition by cs_catalog_page_sk) as groupsize
from catalog_sales order by rand()) tmp
where tmp.rownum <= 1000 or (tmp.rownum > 1000 and rand(unix_timestamp()) < (1000 / 10) / tmp.groupsize);

create table if not exists ws_st_1_100 like tpcds_5000_text.web_sales stored as parquet;
insert overwrite table ws_st_1_100
select
ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit
from
(select *, row_number() over (partition by ws_web_site_sk order by ws_web_site_sk) as rownum, count(*) over (partition by ws_web_site_sk) as groupsize
from web_sales order by rand()) tmp
where tmp.rownum <= 100 or (tmp.rownum > 100 and rand(unix_timestamp()) < (100 / 10) / tmp.groupsize);

create table if not exists ws_st_1_1000 like tpcds_5000_text.web_sales stored as parquet;
insert overwrite table ws_st_1_1000
select
ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit
from
(select *, row_number() over (partition by ws_web_site_sk order by ws_web_site_sk) as rownum, count(*) over (partition by ws_web_site_sk) as groupsize
from web_sales order by rand()) tmp
where tmp.rownum <= 1000 or (tmp.rownum > 1000 and rand(unix_timestamp()) < (1000 / 10) / tmp.groupsize);
