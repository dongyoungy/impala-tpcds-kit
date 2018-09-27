#!/bin/bash

# eval q24
for table in `impala-shell --quiet -B -i cp-1 -q "use tpcds_q24; show tables"`
do
    # print group #
    groupcount=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q24; select count(*) from ${table}"`
    avgpererr=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q24; select avg(abs(quotient(((t.netpaid / t.groupsize * o.groupsize) - o.netpaid) * 100000, o.netpaid) / 100000)) from original o, ${table} t where o.c_last_name = t.c_last_name and o.c_first_name = t.c_first_name and o.s_store_name = t.s_store_name and o.ca_state = t.ca_state and o.s_state = t.s_state and o.i_color = t.i_color and o.i_current_price = t.i_current_price and o.i_manager_id = t.i_manager_id and o.i_units = t.i_units and o.i_size = t.i_size; "`
    echo "${table}, ${groupcount}, ${avgpererr}" >> q24.eval
done

# eval q40
for table in `impala-shell --quiet -B -i cp-1 -q "use tpcds_q40; show tables"`
do
    # print group #
    groupcount=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q40; select count(*) from ${table}"`
    avgpererr=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q40; select avg((abs(((t.sales_before / t.groupsize * o.groupsize) - o.sales_before) / o.sales_before) + abs(((t.sales_after / t.groupsize * o.groupsize) - o.sales_after) / o.sales_after) ) / 2) from original o, ${table} t where o.w_state = t.w_state and o.i_item_id = t.i_item_id"`
    echo "${table}, ${groupcount}, ${avgpererr}" >> q40.eval
done

# eval q50
for table in `impala-shell --quiet -B -i cp-1 -q "use tpcds_q50; show tables"`
do
    # print group #
    groupcount=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q50; select count(*) from ${table}"`
    avgpererr=$(impala-shell --quiet -B -i cp-1 -q "use tpcds_q50; select avg((abs(((t.\`30_days\` / t.groupsize * o.groupsize) - o.\`30_days\`) / o.\`30_days\`) + abs(((t.\`3160_days\` / t.groupsize * o.groupsize) - o.\`3160_days\`) / o.\`3160_days\`) + abs(((t.\`6190_days\` / t.groupsize * o.groupsize) - o.\`6190_days\`) / o.\`6190_days\`) + abs(((t.\`91120_days\` / t.groupsize * o.groupsize) - o.\`91120_days\`) / o.\`91120_days\`) + abs(((t.\`120_days\` / t.groupsize * o.groupsize) - o.\`120_days\`) / o.\`120_days\`) ) / 5) from original o, ${table} t where o.s_store_name = t.s_store_name and o.s_company_id = t.s_company_id and o.s_street_number = t.s_street_number and o.s_street_name = t.s_street_name and o.s_street_type = t.s_street_type and o.s_suite_number = t.s_suite_number and o.s_city = t.s_city and o.s_county = t.s_county and o.s_state = t.s_state and o.s_zip = t.s_zip;")
    echo "${table}, ${groupcount}, ${avgpererr}" >> q50.eval
done

# eval q64
for table in `impala-shell --quiet -B -i cp-1 -q "use tpcds_q64; show tables"`
do
    # print group #
    groupcount=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q64; select count(*) from ${table}"`
    avgpererr=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q64; select avg((abs(quotient(((t.sale / t.groupsize * o.groupsize) - o.sale) * 100000, o.sale) / 100000) + abs(quotient(((t.refund / t.groupsize * o.groupsize) - o.refund) * 100000, o.refund) / 100000) ) / 2) from original o, ${table} t where o.cs_item_sk = t.cs_item_sk; "`
    echo "${table}, ${groupcount}, ${avgpererr}" >> q64.eval
done

# eval q80
for table in `impala-shell --quiet -B -i cp-1 -q "use tpcds_q80; show tables"`
do
    # print group #
    groupcount=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q80; select count(*) from ${table}"`
    avgpererr="-1"
    if [[ $table == q1* ]]; then
        avgpererr=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q80; select avg((abs(quotient(((t.sales / t.groupsize * o.groupsize) - o.sales) * 100000, o.sales) / 100000) + abs(quotient(((t.returns1 / t.groupsize * o.groupsize) - o.returns1) * 100000 , o.returns1) / 100000) + abs(quotient(((t.profit / t.groupsize * o.groupsize) - o.profit) * 100000 , o.profit) / 100000) ) / 3) from orig1 o, ${table} t where o.store_id = t.store_id; "`
    elif [[ $table == q2* ]]; then
        avgpererr=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q80; select avg((abs(quotient(((t.sales / t.groupsize * o.groupsize) - o.sales) * 100000, o.sales) / 100000) + abs(quotient(((t.returns1 / t.groupsize * o.groupsize) - o.returns1) * 100000 , o.returns1) / 100000) + abs(quotient(((t.profit / t.groupsize * o.groupsize) - o.profit) * 100000 , o.profit) / 100000) ) / 3) from orig2 o, ${table} t where o.catalog_page_id = t.catalog_page_id; "`
    elif [[ $table == q3* ]]; then
        avgpererr=`impala-shell --quiet -B -i cp-1 -q "use tpcds_q80; select avg((abs(quotient(((t.sales / t.groupsize * o.groupsize) - o.sales) * 100000, o.sales) / 100000) + abs(quotient(((t.returns1 / t.groupsize * o.groupsize) - o.returns1) * 100000 , o.returns1) / 100000) + abs(quotient(((t.profit / t.groupsize * o.groupsize) - o.profit) * 100000 , o.profit) / 100000) ) / 3) from orig3 o, ${table} t where o.web_site_id = t.web_site_id; "`
    fi
    echo "${table}, ${groupcount}, ${avgpererr}" >> q80.eval
done
