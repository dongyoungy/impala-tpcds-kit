use tpcds_5000_parquet;

create table if not exists cs_st_3_20 like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table cs_st_3_20
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(select *, row_number() over (partition by cs_sold_date_sk, w_state, i_item_id order by w_state, i_item_id) as rownum, count(*) over (partition by cs_sold_date_sk, w_state, i_item_id) as groupsize
from catalog_sales, item, warehouse where cs_warehouse_sk = w_warehouse_sk and cs_item_sk = i_item_sk order by rand()) tmp
where tmp.rownum <= 20 or (tmp.rownum > 20 and rand(unix_timestamp()) < (20 / 10) / tmp.groupsize);


create table if not exists cs_st_3_30 like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table cs_st_3_30
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(select *, row_number() over (partition by cs_sold_date_sk, w_state, i_item_id order by w_state, i_item_id) as rownum, count(*) over (partition by cs_sold_date_sk, w_state, i_item_id) as groupsize
from catalog_sales, item, warehouse where cs_warehouse_sk = w_warehouse_sk and cs_item_sk = i_item_sk order by rand()) tmp
where tmp.rownum <= 30 or (tmp.rownum > 30 and rand(unix_timestamp()) < (30 / 10) / tmp.groupsize);

create table if not exists cs_st_3_50 like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table cs_st_3_50
select
cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit
from
(select *, row_number() over (partition by cs_sold_date_sk, w_state, i_item_id order by w_state, i_item_id) as rownum, count(*) over (partition by cs_sold_date_sk, w_state, i_item_id) as groupsize
from catalog_sales, item, warehouse where cs_warehouse_sk = w_warehouse_sk and cs_item_sk = i_item_sk order by rand()) tmp
where tmp.rownum <= 50 or (tmp.rownum > 50 and rand(unix_timestamp()) < (50 / 10) / tmp.groupsize);
