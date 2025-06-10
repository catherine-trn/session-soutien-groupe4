-- model int_web_session.sql
with session_product as (
   select distinct
       session_id,
       date(created_at) as session_date,
       user_id,
       traffic_source,
       -- Extract product_id from the URI
       cast(regexp_extract(uri, '^/product/(.*)') as integer) as product_id,
       -- Flag if the next event is cart addition
       if(
           lead(event_type) over (partition by session_id order by sequence_number)
           = 'cart',
           true,
           false
       ) as event_cart_add,
       -- Flag if any event in the session was a purchase
       if(
           last_value(event_type) over (
               partition by session_id
               order by sequence_number
               rows between unbounded preceding and unbounded following
           )
           = 'purchase',
           true,
           false
       ) as event_purchase
   from `bigquery-public-data.thelook_ecommerce.events`
   where user_id is not null
)
-- Join the sessions with product details
select distinct
   sp.session_id,
   sp.session_date,
   sp.user_id,
   sp.traffic_source,
   sp.product_id,
   MAX(sp.event_cart_add) AS event_cart_add,
   MAX(sp.event_purchase) AS event_purchase,
   MAX(p.department) AS department,
   MAX(p.category) AS category,
   MAX(p.name) AS name
from session_product as sp
-- Join on product details
left join `bigquery-public-data.thelook_ecommerce.products` as p
   on sp.product_id = p.id
where sp.product_id is not null
group by 1,2,3,4,5