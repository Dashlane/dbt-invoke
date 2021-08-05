SELECT
    CAST(c.created_at AS DATE) AS created_on
    , SUM(o.quantity * i.price) AS total_revenue
FROM
    {{ ref('orders') }} AS o
LEFT JOIN
    {{ ref('customers') }} AS c
    ON o.customer_id = c.customer_id
LEFT JOIN
    {{ ref('items_snapshot') }} AS i
    ON o.item_id = i.item_id
    AND o.order_at >= i.dbt_valid_from
    AND o.order_at < COALESCE(i.dbt_valid_to, CURRENT_TIMESTAMP)
WHERE
    o.order_at >= '1970-01-01 00:00:00'
GROUP BY
    CAST(c.created_at AS DATE)