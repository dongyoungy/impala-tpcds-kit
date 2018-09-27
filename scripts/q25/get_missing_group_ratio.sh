#!/bin/bash
for ss in store_sales store_sales_uf_1p store_sales_uf_10p store_sales_st_100_ss_sold_date_sk store_sales_st_1000_ss_sold_date_sk store_sales_st_10000_ss_sold_date_sk store_sales_st_100_ss_item_sk_ss_store_sk store_sales_st_1000_ss_item_sk_ss_store_sk store_sales_st_10000_ss_item_sk_ss_store_sk store_sales_uv_1p_ss_item_sk_ss_customer_sk_ss_ticket_number store_sales_uv_10p_ss_item_sk_ss_customer_sk_ss_ticket_number
do 
    for sr in store_returns store_returns_uf_1p store_returns_uf_10p store_returns_st_10_sr_item_sk_sr_returned_date_sk store_returns_st_100_sr_returned_date_sk store_returns_st_100_sr_item_sk store_returns_st_1000_sr_returned_date_sk store_returns_st_1000_sr_item_sk store_returns_st_10000_sr_returned_date_sk store_returns_st_10000_sr_item_sk store_returns_uv_1p_sr_item_sk_sr_customer_sk_sr_ticket_number store_returns_uv_10p_sr_item_sk_sr_customer_sk_sr_ticket_number
    do
        for cs in catalog_sales catalog_sales_uf_1p catalog_sales_uf_10p catalog_sales_st_100_cs_item_sk catalog_sales_st_1000_cs_item_sk catalog_sales_st_10000_cs_item_sk catalog_sales_st_100_cs_sold_date_sk catalog_sales_st_1000_cs_sold_date_sk catalog_sales_st_10000_cs_sold_date_sk catalog_sales_st_10_cs_item_sk_cs_sold_date_sk catalog_sales_st_100_cs_item_sk_cs_sold_date_sk
        do 
            table_name=${ss}__${sr}__${cs}
            size=${#table_name}
            echo $size
            if [ $size -lt 128 ]
            then
                impala-shell -B -i cp-1 -q "insert into tpcds_5000_q25_results.missing_group_ratio select '${ss}', '${sr}', '${cs}', count(*), (44265-count(*)) / 44265 from tpcds_5000_q25_results.${ss}__${sr}__${cs}; " 1> test.out 2> test.err
            fi
        done
    done
done
mail -s "Test Done" dyoon@umich.edu < /dev/null
