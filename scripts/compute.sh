#!/bin/bash
impala-shell -q "compute stats tpcds_5000_parquet.store_sales_prejoin1"
