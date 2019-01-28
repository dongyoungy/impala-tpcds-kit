--
-- adjust the source/text schema (tpcds_2000_text)
-- and target/parquet schema (tpcds_2000_parquet)
-- if necessary
--

use tpcds_2000_parquet;

insert overwrite table call_center            select * from tpcds_2000_text.call_center;
insert overwrite table catalog_page           select * from tpcds_2000_text.catalog_page;
insert overwrite table customer               select * from tpcds_2000_text.customer;
insert overwrite table customer_address       select * from tpcds_2000_text.customer_address;
insert overwrite table customer_demographics  select * from tpcds_2000_text.customer_demographics;
insert overwrite table date_dim               select * from tpcds_2000_text.date_dim;
insert overwrite table household_demographics select * from tpcds_2000_text.household_demographics;
insert overwrite table income_band            select * from tpcds_2000_text.income_band;
insert overwrite table item                   select * from tpcds_2000_text.item;
insert overwrite table promotion              select * from tpcds_2000_text.promotion;
insert overwrite table reason                 select * from tpcds_2000_text.reason;
insert overwrite table ship_mode              select * from tpcds_2000_text.ship_mode;
insert overwrite table store                  select * from tpcds_2000_text.store;
insert overwrite table time_dim               select * from tpcds_2000_text.time_dim;
insert overwrite table warehouse              select * from tpcds_2000_text.warehouse;
insert overwrite table web_page               select * from tpcds_2000_text.web_page;
insert overwrite table web_site               select * from tpcds_2000_text.web_site;
insert overwrite table inventory            select * from tpcds_2000_text.inventory;
insert overwrite table catalog_sales            select * from tpcds_2000_text.catalog_sales;
insert overwrite table catalog_returns            select * from tpcds_2000_text.catalog_returns;
insert overwrite table store_sales            select * from tpcds_2000_text.store_sales;
insert overwrite table store_returns            select * from tpcds_2000_text.store_returns;
insert overwrite table web_sales            select * from tpcds_2000_text.web_sales;
insert overwrite table web_returns            select * from tpcds_2000_text.web_returns;

--
-- dimension tables
--
compute stats call_center;
compute stats catalog_page;
compute stats customer;
compute stats customer_address;
compute stats customer_demographics;
compute stats date_dim;
compute stats household_demographics;
compute stats income_band;
compute stats item;
compute stats promotion;
compute stats reason;
compute stats ship_mode;
compute stats store;
compute stats time_dim;
compute stats warehouse;
compute stats web_page;
compute stats web_site;
--
-- fact tables
--
compute stats catalog_returns;
compute stats catalog_sales;
compute stats inventory;
compute stats store_returns;
compute stats store_sales;
compute stats web_returns;
compute stats web_sales;
