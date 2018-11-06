-- start query 1 in stream 0 using template query94.tpl
select
   count(distinct ws_order_number) as "order_count"
  ,sum(ws_ext_ship_cost) as "total_shipping_cost"
  ,sum(ws_net_profit) as "total_net_profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    cast(d_date as timestamp) between cast('1999-05-01' as timestamp) and
           date_add(cast('1999-05-01' as timestamp), 60)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'TX'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and exists (select *
            from web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
limit 100;

-- end query 1 in stream 0 using template query94.tpl