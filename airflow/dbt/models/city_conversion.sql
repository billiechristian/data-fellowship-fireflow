{{ config(materialized='table') }}

with sales_count as (
  select 
    c.city
    , count(*) as total_customer_city
  from {{ source('staging', 'Fact_Table') }} f 
  join {{ source('staging', 'customer') }} c
  on f.customer_id = c.id
  group by 1
)
select 
  c.city
  , total_customer_city
  , count(*) / sc.total_customer_city as sales_conversion
from {{ source('staging', 'Fact_Table') }} f
join {{ source('staging', 'customer') }} c
on f.customer_id = c.id
join sales_count sc
on sc.city = c.city
where f.y = 'yes'
group by 1, 2, sc.total_customer_city
order by 3 desc, 2 desc
