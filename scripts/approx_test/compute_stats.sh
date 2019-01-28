#!/bin/bash
for table in `impala-shell --quiet -B -i cp-1 -q "use tpcds_5000_parquet; show tables"`
do
    impala-shell --quiet -B -i cp-1 -q "use tpcds_5000_parquet; compute stats ${table}"
done
mail -s "Compute Stats Done" dyoon@umich.edu < /dev/null
