#!/bin/bash
for ss in ss_st_q24_1_10 ss_st_q24_1_100 ss_st_q24_2_10 ss_st_q24_2_100
do 
    for sr in store_returns 
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; drop table if exists tpcds_q24.${ss}__${sr}; create table tpcds_q24.${ss}__${sr} stored as parquet as SELECT c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size, Sum(ss_net_profit) netpaid, count(*) as groupsize FROM ${ss} as store_sales, ${sr} as store_returns, store, item, customer, customer_address WHERE ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk AND ss_customer_sk = c_customer_sk AND ss_item_sk = i_item_sk AND ss_store_sk = s_store_sk AND c_birth_country = Upper(ca_country) AND s_zip = ca_zip AND s_market_id = 8 GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size; "
    done
done
mail -s "Test Done" dyoon@umich.edu < /dev/null
