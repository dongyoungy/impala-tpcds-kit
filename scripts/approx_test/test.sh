#!/bin/bash

# q40
for cs in catalog_sales_uf_1p catalog_sales_uf_10p 
do 
    for cr in catalog_returns catalog_returns_uf_1p 
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; CREATE TABLE tpcds_q40.${cs}__${cr} stored as parquet as SELECT w_state , i_item_id , Sum( CASE WHEN ( Cast(d_date AS TIMESTAMP) < Cast ('2002-06-01' AS TIMESTAMP) ) THEN cs_sales_price - COALESCE(cr_refunded_cash,0) ELSE 0 END ) AS sales_before , Sum( CASE WHEN ( Cast(d_date AS TIMESTAMP) >= Cast ('2002-06-01' AS TIMESTAMP) ) THEN cs_sales_price - COALESCE(cr_refunded_cash,0) ELSE 0 END ) AS sales_after, count(*) as groupsize FROM ${cs} as catalog_sales LEFT OUTER JOIN ${cr} as catalog_returns ON ( cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk ) , warehouse , item , date_dim WHERE i_current_price BETWEEN 0.99 AND 1.49 AND i_item_sk = cs_item_sk AND cs_warehouse_sk = w_warehouse_sk AND cs_sold_date_sk = d_date_sk AND cast(d_date as timestamp) BETWEEN (Cast ('2002-06-01' AS TIMESTAMP) - INTERVAL 30 day) AND ( cast ('2002-06-01' AS TIMESTAMP) + INTERVAL 30 day ) GROUP BY w_state, i_item_id ORDER BY w_state, i_item_id;"
    done
done

# q50
for ss in store_sales_uf_1p store_sales_uf_10p 
do 
    for sr in store_returns_uf_1p store_returns_uf_10p
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_q50.${ss}__${sr} stored as parquet as SELECT s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, Sum(CASE WHEN ( sr_returned_date_sk - ss_sold_date_sk <= 30  ) THEN 1 ELSE 0 END) AS 30_days, Sum(CASE WHEN ( sr_returned_date_sk - ss_sold_date_sk > 30  ) AND ( sr_returned_date_sk - ss_sold_date_sk <= 60  ) THEN 1 ELSE 0 END) AS 3160_days, Sum(CASE WHEN ( sr_returned_date_sk - ss_sold_date_sk > 60  ) AND ( sr_returned_date_sk - ss_sold_date_sk <= 90  ) THEN 1 ELSE 0 END) AS 6190_days, Sum(CASE WHEN ( sr_returned_date_sk - ss_sold_date_sk > 90  ) AND ( sr_returned_date_sk - ss_sold_date_sk <= 120  ) THEN 1 ELSE 0 END) AS 91120_days, Sum(CASE WHEN ( sr_returned_date_sk - ss_sold_date_sk > 120  ) THEN 1 ELSE 0 END) AS 120_days, count(*) as groupsize FROM ${ss} as store_sales, ${sr} as store_returns, store, date_dim d1, date_dim d2 WHERE d2.d_year = 2002 AND d2.d_moy = 9 AND ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk AND ss_sold_date_sk = d1.d_date_sk AND sr_returned_date_sk = d2.d_date_sk AND ss_customer_sk = sr_customer_sk AND ss_store_sk = s_store_sk GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip ORDER BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip;"
    done
done

# q64
for cs in catalog_sales_uf_1p catalog_sales_uf_10p 
do 
    for cr in catalog_returns catalog_returns_uf_1p catalog_returns_uf_10p
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_q64.${cs}__${cr} stored as parquet as SELECT cs_item_sk, Sum(cs_ext_list_price) AS sale, Sum(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund , count(*) as groupsize FROM ${cs} as catalog_sales, ${cr} as catalog_returns WHERE cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number GROUP BY cs_item_sk HAVING Sum(cs_ext_list_price) > 2 * Sum( cr_refunded_cash + cr_reversed_charge + cr_store_credit );"
    done
done

# q80-1
for ss in store_sales_uf_1p store_sales_uf_10p store_sales_q75_uv_1p store_sales_q75_uv_10p
do 
    for sr in store_returns_uf_1p store_returns_uf_10p store_returns_q75_uv_1p store_returns_q75_uv_10p 
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_q80.q1_${ss}__${sr} stored as parquet as SELECT s_store_id AS store_id, Sum(ss_ext_sales_price) AS sales, Sum(COALESCE(sr_return_amt, 0)) AS returns1, Sum(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit, count(*) as groupsize FROM ${ss} as store_sales LEFT OUTER JOIN ${sr} as store_returns ON ( ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number ), date_dim, store, item, promotion WHERE ss_sold_date_sk = d_date_sk AND cast(d_date as timestamp) BETWEEN Cast('2000-08-26' AS timestamp) AND ( Cast('2000-08-26' AS timestamp) + INTERVAL 30 day ) AND ss_store_sk = s_store_sk AND ss_item_sk = i_item_sk AND i_current_price > 50 AND ss_promo_sk = p_promo_sk AND p_channel_tv = 'N' GROUP BY s_store_id; "
    done
done

# q80-2
for cs in catalog_sales_uf_1p catalog_sales_uf_10p 
do 
    for cr in catalog_returns catalog_returns_uf_1p catalog_returns_uf_10p
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_q80.q2_${cs}__${cr} stored as parquet as SELECT cp_catalog_page_id AS catalog_page_id, sum(cs_ext_sales_price) AS sales, sum(COALESCE(cr_return_amount, 0)) AS returns1, sum(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit, count(*) as groupsize FROM ${cs} as catalog_sales LEFT OUTER JOIN ${cr} as catalog_returns ON ( cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number ), date_dim, catalog_page, item, promotion WHERE cs_sold_date_sk = d_date_sk AND cast(d_date as timestamp) BETWEEN cast('2000-08-26' AS timestamp) AND ( cast('2000-08-26' AS timestamp) + INTERVAL 30 day ) AND cs_catalog_page_sk = cp_catalog_page_sk AND cs_item_sk = i_item_sk AND i_current_price > 50 AND cs_promo_sk = p_promo_sk AND p_channel_tv = 'N' GROUP BY cp_catalog_page_id;"
    done
done

# q80-3
for ws in web_sales_uf_1p web_sales_uf_10p 
do 
    for wr in web_returns web_returns_uf_1p web_returns_uf_10p
    do
        impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_q80.q3_${ws}__${wr} stored as parquet as SELECT web_site_id, sum(ws_ext_sales_price) AS sales, sum(COALESCE(wr_return_amt, 0)) AS returns1, sum(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit, count(*) as groupsize FROM ${ws} as web_sales LEFT OUTER JOIN ${wr} as web_returns ON ( ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number ), date_dim, web_site, item, promotion WHERE ws_sold_date_sk = d_date_sk AND cast(d_date as timestamp) BETWEEN cast('2000-08-26' AS timestamp) AND ( cast('2000-08-26' AS timestamp) + INTERVAL 30 day ) AND ws_web_site_sk = web_site_sk AND ws_item_sk = i_item_sk AND i_current_price > 50 AND ws_promo_sk = p_promo_sk AND p_channel_tv = 'N' GROUP BY web_site_id;"
    done
done


mail -s "Test Done" dyoon@umich.edu < /dev/null
