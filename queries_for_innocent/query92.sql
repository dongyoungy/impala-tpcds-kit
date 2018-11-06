-- start query 1 in stream 0 using template query92.tpl
select
   sum(ws_ext_discount_amt)  as "Excess_Discount_Amount"
from
    web_sales
   ,item
   ,date_dim
where
i_manufact_id = 269
and i_item_sk = ws_item_sk
and cast(d_date as timestamp) between cast('1998-03-18' as timestamp) and
        date_add(cast('1998-03-18' as timestamp), 90)
and d_date_sk = ws_sold_date_sk
and ws_ext_discount_amt
     > (
         SELECT
            1.3 * avg(ws_ext_discount_amt)
         FROM
            web_sales
           ,date_dim
         WHERE
              ws_item_sk = i_item_sk
          and cast(d_date as timestamp) between cast('1998-03-18' as timestamp) and
                             date_add(cast('1998-03-18' as timestamp),90)
          and d_date_sk = ws_sold_date_sk
      )
order by sum(ws_ext_discount_amt)
limit 100;

-- end query 1 in stream 0 using template query92.tpl
