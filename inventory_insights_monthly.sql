-- model inventory_insights_monthly.sql
with inventory_monthly_inputs as (
       select
           product_distribution_center_id as distribution_center_id,
           format_timestamp('%Y-%m', created_at) as month,
           count(*) as received_items,
           sum(cost) as received_items_value
       from `bigquery-public-data.thelook_ecommerce.inventory_items`
       group by 1, 2
   ),

   inventory_monthly_outputs as (
       select
           product_distribution_center_id as distribution_center_id,
           format_timestamp('%Y-%m', sold_at) as month,
           count(*) as sold_items,
           sum(cost) as sold_items_value
       from `bigquery-public-data.thelook_ecommerce.inventory_items`
       group by 1, 2
   ),

   inventory_monthly_flow as (
       select
           coalesce(inputs.month, outputs.month) as month,
           inputs.distribution_center_id,
           ifnull(received_items, 0) as received_items,
           ifnull(received_items_value, 0) as received_items_value,
           ifnull(sold_items, 0) as sold_items,
           ifnull(sold_items_value, 0) as sold_items_value
       from inventory_monthly_inputs inputs
       left join inventory_monthly_outputs outputs
           on inputs.month = outputs.month
           and inputs.distribution_center_id = outputs.distribution_center_id
   ),

   inventory_monthly as (
       -- Calculer l'opening et closing inventory
       select
           month,
           distribution_center_id,
           received_items,
           received_items_value,
           sold_items,
           sold_items_value,
           cum_received_items - cum_sold_items as closing_inventory,
           lag(cum_received_items - cum_sold_items) over (
               partition by distribution_center_id order by month
           ) as opening_inventory
       from
           (select
                month,
                distribution_center_id,
                received_items,
                received_items_value,
                sold_items,
                sold_items_value,
                -- Somme cumulée des items reçus
                sum(received_items) over (
                    partition by distribution_center_id
                    order by month
                    rows between unbounded preceding and current row
                ) as cum_received_items,
                -- Somme cumulée des items vendus
                sum(sold_items) over (
                    partition by distribution_center_id
                    order by month
                    rows between unbounded preceding and current row
                ) as cum_sold_items
            from inventory_monthly_flow
           )
   )

select
   month,
   distribution_center_id,
   opening_inventory,
   received_items,
   round(received_items_value, 2) as received_items_value,
   sold_items,
   round(sold_items_value, 2) as sold_items_value,
   closing_inventory,
   round(
       safe_divide(sold_items, (opening_inventory + closing_inventory) / 2), 2
   ) as turnover_rate
from inventory_monthly
order by month
