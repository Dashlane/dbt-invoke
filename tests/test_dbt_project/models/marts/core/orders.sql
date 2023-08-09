SELECT
    order_id
    , customer_id
    , item_id
    , quantity
    , order_at
FROM
    {{ source('external_source', 'orders') }}