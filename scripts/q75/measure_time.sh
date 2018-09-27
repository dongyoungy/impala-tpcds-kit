#!/bin/bash
#for ss in store_sales store_sales_uf_1p store_sales_uf_10p store_sales_q75_uv_1p store_sales_q75_uv_10p store_sales_q75_st_g_100 store_sales_q75_st_p_1000 store_sales_q75_st_g_10 store_sales_q75_st_p_10
for ss in  store_sales_q75_st_d_year_10 store_sales_q75_st_d_year_100 store_sales_q75_st_d_year_1000 store_sales_q75_st_d_year_i_manufact_id_10  store_sales_q75_st_d_year_i_manufact_id_100 store_sales_q75_st_d_year_i_manufact_id_1000  store_sales_q75_st_d_year_i_category_id_10  store_sales_q75_st_d_year_i_category_id_100 store_sales_q75_st_d_year_i_category_id_1000
do 
    for sr in store_returns store_returns_uf_1p store_returns_uf_10p store_returns_q75_uv_1p store_returns_q75_uv_10p
    do
        # clear cache
        echo "clearing cache..."
        let "count=0"
        for node in cp-1 cp-2 cp-3 cp-4 cp-5 cp-6 cp-7 cp-8 cp-9 cp-10 cp-11 cp-12 cp-13 cp-14 cp-15 cp-16 cp-17
        do
            (ssh -o "StrictHostKeyChecking no" -p 22 $node "sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches") 1> /dev/null &
        done
        wait
        echo "cache cleared"
        sleep 30
        START=$(date +%s.%N)
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; SELECT d_year ,i_brand_id ,i_class_id ,i_category_id ,i_manufact_id ,sum(ss_quantity - COALESCE(sr_return_quantity,0)) AS sales_cnt ,sum(ss_ext_sales_price - COALESCE(sr_return_amt,0.0)) AS sales_amt ,count(*) as groupsize FROM ${ss} as store_sales JOIN item ON i_item_sk=ss_item_sk JOIN date_dim ON d_date_sk=ss_sold_date_sk LEFT JOIN ${sr} as store_returns ON (ss_ticket_number=sr_ticket_number AND ss_item_sk=sr_item_sk) WHERE i_category='Sports' GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id;" -o impala.out
        END=$(date +%s.%N)
        runtime=$(echo "$END - $START" | bc)
        echo "${ss} ${sr} ${runtime}" >> runtime.out

    #impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; select count(*) from tpcds_5000_q75_result.${ss}__${sr}" -o impala.out
    done
done
