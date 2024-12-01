{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        `Courier Status` AS courier_status,
        `ship-service-level` AS service_level,
        `ship-city` AS ship_city,
        `ship-state` AS ship_state,
        `ship-postal-code` AS ship_postal_code,
        `ship-country` AS ship_country
    FROM {{ source('bronze', 'amazon_sale_report') }}
)

SELECT
    ROW_NUMBER() OVER() AS shipment_id,
    courier_status,
    service_level,
    ship_city,
    ship_state,
    ship_postal_code,
    ship_country
FROM source