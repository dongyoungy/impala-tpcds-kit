-- start query 1 in stream 0 using template query16.tpl
select
   count(distinct cs_order_number) as "order_count"
  ,sum(cs_ext_ship_cost) as "total_shipping_cost"
  ,sum(cs_net_profit) as "total_net_profit"
from
   catalog_sales cs1
  ,date_dim
  ,customer_address
  ,call_center
where
    cast(d_date as timestamp) between cast('1999-04-01' as timestamp) and
           date_add(cast('1999-04-01' as timestamp),60)
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'IA'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Ziebach County','Maverick County','Huron County','Kittitas County',
                  'Fairfield County'
)
and exists (select *
            from catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
limit 100;

-- end query 1 in stream 0 using template query16.tpl
