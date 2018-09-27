#!/bin/bash
ss='store_sales_union1'
sr='store_returns'
cs='catalog_sales'
impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_5000_q25_results.${ss}__${sr}__${cs} as select i_item_id ,i_item_desc ,s_store_id ,s_store_name ,sum(ss_net_profit) as store_sales_profit ,sum(sr_net_loss) as store_returns_loss ,sum(cs_net_profit) as catalog_sales_profit, count(*) as groupsize from ${ss} as store_sales ,${sr} as store_returns ,${cs} as catalog_sales ,date_dim d1 ,date_dim d2 ,date_dim d3 ,store ,item where d1.d_moy = 4 and d1.d_year = 2000 and d1.d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and ss_customer_sk = sr_customer_sk and ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number and sr_returned_date_sk = d2.d_date_sk and d2.d_moy between 4 and 10 and d2.d_year = 2000 and sr_customer_sk = cs_bill_customer_sk and sr_item_sk = cs_item_sk and cs_sold_date_sk = d3.d_date_sk and d3.d_moy between 4 and 10 and d3.d_year = 2000 group by i_item_id ,i_item_desc ,s_store_id ,s_store_name order by i_item_id ,i_item_desc ,s_store_id ,s_store_name;"
impala-shell -B -i cp-1 -q "insert into tpcds_5000_q25_results.eval select '${ss}', '${sr}', '${cs}', count(*), (44265-count(*)) / 44265, avg((abs(((t.store_sales_profit / t.groupsize * orig.groupsize) - orig.store_sales_profit) / orig.store_sales_profit) + abs(((t.store_returns_loss / t.groupsize * orig.groupsize) - orig.store_returns_loss) / orig.store_returns_loss) + abs(((t.catalog_sales_profit / t.groupsize * orig.groupsize) - orig.catalog_sales_profit) / orig.catalog_sales_profit)) / 3) from tpcds_5000_q25_results.${ss}__${sr}__${cs} as t, tpcds_5000_q25_results.original as orig where t.i_item_id = orig.i_item_id and t.i_item_desc = orig.i_item_desc and t.s_store_id = orig.s_store_id and t.s_store_name = orig.s_store_name; " 1>> test.out 2>> test.err

ss='store_sales'
sr='store_returns_union1'
cs='catalog_sales'
impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_5000_q25_results.${ss}__${sr}__${cs} as select i_item_id ,i_item_desc ,s_store_id ,s_store_name ,sum(ss_net_profit) as store_sales_profit ,sum(sr_net_loss) as store_returns_loss ,sum(cs_net_profit) as catalog_sales_profit, count(*) as groupsize from ${ss} as store_sales ,${sr} as store_returns ,${cs} as catalog_sales ,date_dim d1 ,date_dim d2 ,date_dim d3 ,store ,item where d1.d_moy = 4 and d1.d_year = 2000 and d1.d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and ss_customer_sk = sr_customer_sk and ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number and sr_returned_date_sk = d2.d_date_sk and d2.d_moy between 4 and 10 and d2.d_year = 2000 and sr_customer_sk = cs_bill_customer_sk and sr_item_sk = cs_item_sk and cs_sold_date_sk = d3.d_date_sk and d3.d_moy between 4 and 10 and d3.d_year = 2000 group by i_item_id ,i_item_desc ,s_store_id ,s_store_name order by i_item_id ,i_item_desc ,s_store_id ,s_store_name;"
impala-shell -B -i cp-1 -q "insert into tpcds_5000_q25_results.eval select '${ss}', '${sr}', '${cs}', count(*), (44265-count(*)) / 44265, avg((abs(((t.store_sales_profit / t.groupsize * orig.groupsize) - orig.store_sales_profit) / orig.store_sales_profit) + abs(((t.store_returns_loss / t.groupsize * orig.groupsize) - orig.store_returns_loss) / orig.store_returns_loss) + abs(((t.catalog_sales_profit / t.groupsize * orig.groupsize) - orig.catalog_sales_profit) / orig.catalog_sales_profit)) / 3) from tpcds_5000_q25_results.${ss}__${sr}__${cs} as t, tpcds_5000_q25_results.original as orig where t.i_item_id = orig.i_item_id and t.i_item_desc = orig.i_item_desc and t.s_store_id = orig.s_store_id and t.s_store_name = orig.s_store_name; " 1>> test.out 2>> test.err

ss='store_sales'
sr='store_returns'
cs='catalog_sales_union1'
impala-shell -B -i cp-1 -q "use tpcds_5000_parquet; create table tpcds_5000_q25_results.${ss}__${sr}__${cs} as select i_item_id ,i_item_desc ,s_store_id ,s_store_name ,sum(ss_net_profit) as store_sales_profit ,sum(sr_net_loss) as store_returns_loss ,sum(cs_net_profit) as catalog_sales_profit, count(*) as groupsize from ${ss} as store_sales ,${sr} as store_returns ,${cs} as catalog_sales ,date_dim d1 ,date_dim d2 ,date_dim d3 ,store ,item where d1.d_moy = 4 and d1.d_year = 2000 and d1.d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and ss_customer_sk = sr_customer_sk and ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number and sr_returned_date_sk = d2.d_date_sk and d2.d_moy between 4 and 10 and d2.d_year = 2000 and sr_customer_sk = cs_bill_customer_sk and sr_item_sk = cs_item_sk and cs_sold_date_sk = d3.d_date_sk and d3.d_moy between 4 and 10 and d3.d_year = 2000 group by i_item_id ,i_item_desc ,s_store_id ,s_store_name order by i_item_id ,i_item_desc ,s_store_id ,s_store_name;"
impala-shell -B -i cp-1 -q "insert into tpcds_5000_q25_results.eval select '${ss}', '${sr}', '${cs}', count(*), (44265-count(*)) / 44265, avg((abs(((t.store_sales_profit / t.groupsize * orig.groupsize) - orig.store_sales_profit) / orig.store_sales_profit) + abs(((t.store_returns_loss / t.groupsize * orig.groupsize) - orig.store_returns_loss) / orig.store_returns_loss) + abs(((t.catalog_sales_profit / t.groupsize * orig.groupsize) - orig.catalog_sales_profit) / orig.catalog_sales_profit)) / 3) from tpcds_5000_q25_results.${ss}__${sr}__${cs} as t, tpcds_5000_q25_results.original as orig where t.i_item_id = orig.i_item_id and t.i_item_desc = orig.i_item_desc and t.s_store_id = orig.s_store_id and t.s_store_name = orig.s_store_name; " 1>> test.out 2>> test.err

mail -s "Test Done" dyoon@umich.edu < /dev/null
