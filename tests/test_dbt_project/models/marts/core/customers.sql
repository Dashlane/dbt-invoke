SELECT
    customer_id
    , created_at
FROM
    {{ source('external_source', 'customers') }}