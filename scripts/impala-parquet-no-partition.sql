--
-- adjust the schema name if necessary
-- currently (tpcds_5000_parquet)
--

create schema tpcds_5000_parquet;
use tpcds_5000_parquet;

--
-- unpartitioned tables
--
create table call_center            like tpcds_5000_text.call_center            stored as parquet;
create table catalog_page           like tpcds_5000_text.catalog_page           stored as parquet;
create table customer               like tpcds_5000_text.customer               stored as parquet;
create table customer_address       like tpcds_5000_text.customer_address       stored as parquet;
create table customer_demographics  like tpcds_5000_text.customer_demographics  stored as parquet;
create table date_dim               like tpcds_5000_text.date_dim               stored as parquet;
create table household_demographics like tpcds_5000_text.household_demographics stored as parquet;
create table income_band            like tpcds_5000_text.income_band            stored as parquet;
create table item                   like tpcds_5000_text.item                   stored as parquet;
create table promotion              like tpcds_5000_text.promotion              stored as parquet;
create table reason                 like tpcds_5000_text.reason                 stored as parquet;
create table ship_mode              like tpcds_5000_text.ship_mode              stored as parquet;
create table store                  like tpcds_5000_text.store                  stored as parquet;
create table time_dim               like tpcds_5000_text.time_dim               stored as parquet;
create table warehouse              like tpcds_5000_text.warehouse              stored as parquet;
create table web_page               like tpcds_5000_text.web_page               stored as parquet;
create table web_site               like tpcds_5000_text.web_site               stored as parquet;
create table inventory            like tpcds_5000_text.inventory               stored as parquet;
create table store_sales  like tpcds_5000_text.store_sales               stored as parquet;
create table store_returns  like tpcds_5000_text.store_returns               stored as parquet;
create table catalog_returns  like tpcds_5000_text.catalog_returns               stored as parquet;
create table catalog_sales  like tpcds_5000_text.catalog_sales               stored as parquet;
create table web_returns  like tpcds_5000_text.web_returns               stored as parquet;
create table web_sales  like tpcds_5000_text.web_sales               stored as parquet;
