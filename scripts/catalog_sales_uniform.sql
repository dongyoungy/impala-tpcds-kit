use tpcds_5000_parquet;
create table catalog_sales_uf_1p like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table catalog_sales_uf_1p select * from catalog_sales where rand(unix_timestamp()) < 0.01;
create table catalog_sales_uf_10p like tpcds_5000_text.catalog_sales stored as parquet;
insert overwrite table catalog_sales_uf_10p select * from catalog_sales where rand(unix_timestamp()) < 0.1;
