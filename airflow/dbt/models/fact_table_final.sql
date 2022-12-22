{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
)}}


select f.*, c.*, s.id as _sales_id, s.name as sales_name, s.age as sales_age, s.city as sales_city, s.sex as sales_sex
from {{ source('staging', 'Fact_Table') }} f
left join {{ source('staging', 'customer') }} c
on f.customer_id = c.id
left join {{ source('staging', 'salesperson') }} s
on f.sales_id = CAST(s.id AS STRING)
