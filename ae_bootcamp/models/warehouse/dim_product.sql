WITH staging_source AS (
  SELECT
      product_id
    , product_code
    , product_name
    , description
    , supplier_company
    , standard_cost
    , list_price
    , reorder_level
    , target_level
    , quantity_per_unit
    , discontinued
    , minimum_reorder_quantity
    , category
    , attachments
    , current_timestamp() as ingestion_timestamp
    , FROM {{ ref('stg_products') }}
),
unique_source AS (
    SELECT
      *
    , row_number() OVER (PARTITION BY product_id) AS row_number
    FROM staging_source
)
SELECT *
EXCEPT (row_number),
FROM unique_source
WHERE row_number = 1
