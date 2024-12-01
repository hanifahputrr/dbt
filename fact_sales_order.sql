{{ config(materialized='table') }}

WITH source AS (
    SELECT
        `Order ID` AS order_id,
        Date AS order_date,
        Qty AS quantity,
        Amount AS amount,
        currency,
        ROW_NUMBER() OVER(PARTITION BY Fulfilment) AS fulfilment_id,
        ROW_NUMBER() OVER(PARTITION BY Style) AS product_id,
        ROW_NUMBER() OVER(PARTITION BY `promotion-ids`) AS promotion_id_key,
        ROW_NUMBER() OVER(PARTITION BY `Courier Status`) AS shipment_id,
        ROW_NUMBER() OVER(PARTITION BY `Sales Channel `) AS channel_id
    FROM {{ source('bronze', 'amazon_sale_report') }}
)

SELECT
    order_id,
    order_date,
    quantity,
    amount,
    currency,
    fulfilment_id,
    product_id,
    promotion_id_key,
    shipment_id,
    channel_id
FROM source