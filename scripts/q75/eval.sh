#!/bin/bash
#for ss in store_sales store_sales_uf_1p store_sales_uf_10p store_sales_q75_uv_1p store_sales_q75_uv_10p store_sales_q75_st_g_100 store_sales_q75_st_p_1000 store_sales_q75_st_p_10000
#for ss in store_sales_q75_st_g_10 store_sales_q75_st_p_10
for ss in  store_sales_q75_st_d_year_10 store_sales_q75_st_d_year_100 store_sales_q75_st_d_year_1000 store_sales_q75_st_d_year_i_manufact_id_10  store_sales_q75_st_d_year_i_manufact_id_100 store_sales_q75_st_d_year_i_manufact_id_1000  store_sales_q75_st_d_year_i_category_id_10  store_sales_q75_st_d_year_i_category_id_100 store_sales_q75_st_d_year_i_category_id_1000
do 
    for sr in store_returns store_returns_uf_1p store_returns_uf_10p store_returns_q75_uv_1p store_returns_q75_uv_10p
    do
        table_name=${ss}__${sr}
        size=${#table_name}
        echo $size
        if [ $size -lt 128 ]
        then
            impala-shell -B -i cp-1 -q "insert into tpcds_5000_q75_result.eval select '${ss}', '${sr}', count(*), (9329-count(*)) / 9329, avg((abs(((t.sales_cnt / t.groupsize * orig.groupsize) - orig.sales_cnt) / orig.sales_cnt) + abs(((t.sales_amt / t.groupsize * orig.groupsize) - orig.sales_amt) / orig.sales_amt) ) / 2) from tpcds_5000_q75_result.${ss}__${sr} as t, tpcds_5000_q75_result.original as orig where t.d_year = orig.d_year and t.i_brand_id = orig.i_brand_id and t.i_class_id = orig.i_class_id and t.i_category_id = orig.i_category_id and t.i_manufact_id = orig.i_manufact_id; " 1>> test.out 2>> test.err
        fi
    done
done
mail -s "Test Done" dyoon@umich.edu < /dev/null
