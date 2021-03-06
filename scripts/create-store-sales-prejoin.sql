use tpcds_5000_parquet;
create table if not exists store_sales_prejoin1 (
  ss_sold_date_sk int,
  ss_sold_time_sk int,
  ss_item_sk int,
  ss_customer_sk int,
  ss_cdemo_sk int,
  ss_hdemo_sk int,
  ss_addr_sk int,
  ss_store_sk int,
  ss_promo_sk int,
  ss_ticket_number bigint,
  ss_quantity int,
  ss_wholesale_cost decimal(7,2),
  ss_list_price decimal(7,2),
  ss_sales_price decimal(7,2),
  ss_ext_discount_amt decimal(7,2),
  ss_ext_sales_price decimal(7,2),
  ss_ext_wholesale_cost decimal(7,2),
  ss_ext_list_price decimal(7,2),
  ss_ext_tax decimal(7,2),
  ss_coupon_amt decimal(7,2),
  ss_net_paid decimal(7,2),
  ss_net_paid_inc_tax decimal(7,2),
  ss_net_profit decimal(7,2),
  d_date_sk int,
  d_date_id varchar(16),
  d_date varchar(10),
  d_month_seq int,
  d_week_seq int,
  d_quarter_seq int,
  d_year int,
  d_dow int,
  d_moy int,
  d_dom int,
  d_qoy int,
  d_fy_year int,
  d_fy_quarter_seq int,
  d_fy_week_seq int,
  d_day_name varchar(9),
  d_quarter_name varchar(6),
  d_holiday varchar(1),
  d_weekend varchar(1),
  d_following_holiday varchar(1),
  d_first_dom int,
  d_last_dom int,
  d_same_day_ly int,
  d_same_day_lq int,
  d_current_day varchar(1),
  d_current_week varchar(1),
  d_current_month varchar(1),
  d_current_quarter varchar(1),
  d_current_year varchar(1),
  i_item_sk int,
  i_item_id varchar(16),
  i_rec_start_date varchar(10),
  i_rec_end_date varchar(10),
  i_item_desc varchar(200),
  i_current_price decimal(7,2),
  i_wholesale_cost decimal(7,2),
  i_brand_id int,
  i_brand varchar(50),
  i_class_id int,
  i_class varchar(50),
  i_category_id int,
  i_category varchar(50),
  i_manufact_id int,
  i_manufact varchar(50),
  i_size varchar(20),
  i_formulation varchar(20),
  i_color varchar(20),
  i_units varchar(10),
  i_container varchar(10),
  i_manager_id int,
  i_product_name varchar(50),
  s_store_sk int,
  s_store_id varchar(16),
  s_rec_start_date varchar(10),
  s_rec_end_date varchar(10),
  s_closed_date_sk int,
  s_store_name varchar(50),
  s_number_employees int,
  s_floor_space int,
  s_hours varchar(20),
  s_manager varchar(40),
  s_market_id int,
  s_geography_class varchar(100),
  s_market_desc varchar(100),
  s_market_manager varchar(40),
  s_division_id int,
  s_division_name varchar(50),
  s_company_id int,
  s_company_name varchar(50),
  s_street_number varchar(10),
  s_street_name varchar(60),
  s_street_type varchar(15),
  s_suite_number varchar(10),
  s_city varchar(60),
  s_county varchar(30),
  s_state varchar(2),
  s_zip varchar(10),
  s_country varchar(20),
  s_gmt_offset decimal(5,2),
  s_tax_precentage decimal(5,2),
  v_rand double
)
stored as parquet;

insert overwrite table store_sales_prejoin1 select ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit,d_date_sk,d_date_id,d_date,d_month_seq,d_week_seq,d_quarter_seq,d_year,d_dow,d_moy,d_dom,d_qoy,d_fy_year,d_fy_quarter_seq,d_fy_week_seq,d_day_name,d_quarter_name,d_holiday,d_weekend,d_following_holiday,d_first_dom,d_last_dom,d_same_day_ly,d_same_day_lq,d_current_day,d_current_week,d_current_month,d_current_quarter,d_current_year,i_item_sk,i_item_id,i_rec_start_date,i_rec_end_date,i_item_desc,i_current_price,i_wholesale_cost,i_brand_id,i_brand,i_class_id,i_class,i_category_id,i_category,i_manufact_id,i_manufact,i_size,i_formulation,i_color,i_units,i_container,i_manager_id,i_product_name,s_store_sk,s_store_id,s_rec_start_date,s_rec_end_date,s_closed_date_sk,s_store_name,s_number_employees,s_floor_space,s_hours,s_manager,s_market_id,s_geography_class,s_market_desc,s_market_manager,s_division_id,s_division_name,s_company_id,s_company_name,s_street_number,s_street_name,s_street_type,s_suite_number,s_city,s_county,s_state,s_zip,s_country,s_gmt_offset,s_tax_precentage, rand(unix_timestamp()) as v_rand from store_sales, date_dim, item, store where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_store_sk = s_store_sk;
