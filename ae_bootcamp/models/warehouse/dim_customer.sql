WITH staging_source AS (
  SELECT
    id as unique_customer_id,
    company,
    last_name,
    first_name,
    email_address,
    job_title
    business_phone,
    home_phone,
    mobile_phone,
    fax_number,
    address,
    city,
    state_province,
    zip_postal_code,
    country_region,
    web_page,
    notes,
    attachments,
    ingestion_timestamp
  FROM {{ ref('stg_customer') }}
),
unique_source AS (
    SELECT
      *
    , row_number() OVER (PARTITION BY unique_customer_id) AS row_number
    FROM staging_source
)
SELECT *
EXCEPT (row_number),
FROM unique_source
WHERE row_number = 1
