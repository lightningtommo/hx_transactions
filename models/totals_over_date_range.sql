SELECT
  o.order_transaction_timestamp,
  l.line_item_date,
  l.line_item_type,
  SUM(line_item_gross) AS total_gross
FROM
  {{ ref('orders_line_items') }} o
CROSS JOIN
  o.line_items l
GROUP BY
  1,2,3
ORDER BY
  1,2,3
