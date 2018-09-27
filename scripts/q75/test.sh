#!/bin/bash
#for ss in store_sales store_sales_uf_1p store_sales_uf_10p store_sales_q75_uv_1p store_sales_q75_uv_10p store_sales_q75_st_g_100 store_sales_q75_st_p_1000 store_sales_q75_st_g_10 store_sales_q75_st_p_10 store_sales_q75_st_d_year_10 store_sales_q75_st_d_year_100 store_sales_q75_st_d_year_1000 store_sales_q75_st_d_year_i_manufact_id_10  store_sales_q75_st_d_year_i_manufact_id_100 store_sales_q75_st_d_year_i_manufact_id_1000  store_sales_q75_st_d_year_i_category_id_10  store_sales_q75_st_d_year_i_category_id_100 store_sales_q75_st_d_year_i_category_id_1000
for ss in  store_sales_q75_st_d_year_10 store_sales_q75_st_d_year_100 store_sales_q75_st_d_year_1000 store_sales_q75_st_d_year_i_manufact_id_10  store_sales_q75_st_d_year_i_manufact_id_100 store_sales_q75_st_d_year_i_manufact_id_1000  store_sales_q75_st_d_year_i_category_id_10  store_sales_q75_st_d_year_i_category_id_100 store_sales_q75_st_d_year_i_category_id_1000
do 
    for sr in store_returns store_returns_uf_1p store_returns_uf_10p store_returns_q75_uv_1p store_returns_q75_uv_10p
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_5000_q75_result.${ss}__${sr} as SELECT d_year ,i_brand_id ,i_class_id ,i_category_id ,i_manufact_id ,sum(ss_quantity - COALESCE(sr_return_quantity,0)) AS sales_cnt ,sum(ss_ext_sales_price - COALESCE(sr_return_amt,0.0)) AS sales_amt ,count(*) as groupsize FROM ${ss} as store_sales JOIN item ON i_item_sk=ss_item_sk JOIN date_dim ON d_date_sk=ss_sold_date_sk LEFT JOIN ${sr} as store_returns ON (ss_ticket_number=sr_ticket_number AND ss_item_sk=sr_item_sk) WHERE i_category='Sports' GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id;"
    done
done
mail -s "Test Done" dyoon@umich.edu < /dev/null
