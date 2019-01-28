#!/bin/bash
impala-shell -f ./impala-external.sql
impala-shell -f ./impala-parquet-no-partition.sql
impala-shell -f ./impala-insert-no-partitions.sql
mail -s "Run Complete" dyoon@umich.edu < /dev/null
