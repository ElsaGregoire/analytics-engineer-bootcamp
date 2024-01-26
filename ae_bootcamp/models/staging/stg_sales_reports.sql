with source as (
  select * from {{ source('northwind', 'sales_reports') }}
)
select * from source
