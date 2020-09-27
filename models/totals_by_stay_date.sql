SELECT
  l.line_item_date,
  SUM(line_item_gross) AS total_gross
FROM
  {{ ref('orders_line_items') }} o
CROSS JOIN
  o.line_items l
GROUP BY
  1
ORDER BY
  1
