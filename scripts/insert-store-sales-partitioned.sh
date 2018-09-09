#!/bin/bash
numpart=1000000
for i in {1..100}
do
minval=$(( (i-1) * 10000))
maxval=$(( i * 10000))
echo "inserting partition from ${minval} to ${maxval}"
impala-shell -q "use tpcds_500_parquet; INSERT INTO TABLE store_sales_part${numpart} PARTITION(ss_part) SELECT ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit,cast((ss_part%${numpart})+1 as int) as ss_part FROM store_sales_part1000000_temp WHERE ss_part > ${minval} and ss_part <= ${maxval};"
done
