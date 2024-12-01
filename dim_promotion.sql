{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        `promotion-ids` AS promotion_id
    FROM {{ source('bronze', 'amazon_sale_report') }}
)

SELECT
    ROW_NUMBER() OVER() AS promotion_id_key,
    promotion_id
FROM source