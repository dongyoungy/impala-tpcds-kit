create table if not exists test.store_sales_partitioned_d_year (
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
  ss_net_profit decimal(7,2)
)
partitioned by (d_year int)
stored as parquet;

insert overwrite table test.store_sales_partitioned_d_year partition (d_year) select ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit,d_year from tpcds_500_parquet_innocent.prejoin__date_dim__store_sales;
