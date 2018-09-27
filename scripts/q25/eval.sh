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
                impala-shell -B -i cp-1 -q "insert into tpcds_5000_q25_results.eval select '${ss}', '${sr}', '${cs}', count(*), (44265-count(*)) / 44265, avg((abs(((t.store_sales_profit / t.groupsize * orig.groupsize) - orig.store_sales_profit) / orig.store_sales_profit) + abs(((t.store_returns_loss / t.groupsize * orig.groupsize) - orig.store_returns_loss) / orig.store_returns_loss) + abs(((t.catalog_sales_profit / t.groupsize * orig.groupsize) - orig.catalog_sales_profit) / orig.catalog_sales_profit)) / 3) from tpcds_5000_q25_results.${ss}__${sr}__${cs} as t, tpcds_5000_q25_results.original as orig where t.i_item_id = orig.i_item_id and t.i_item_desc = orig.i_item_desc and t.s_store_id = orig.s_store_id and t.s_store_name = orig.s_store_name; " 1>> test.out 2>> test.err
            fi
        done
    done
done
mail -s "Test Done" dyoon@umich.edu < /dev/null
