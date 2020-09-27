/*
Get the state of orders and their line items at a given point in time.
This would be materialized for efficiency.
Would use the real tables _PARTITIONDATE to limit data and transaction date cluster
*/

  WITH orders AS (
    WITH sorted_orders AS (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_version DESC) AS row_no,
        order_id,
        order_version,
        order_transaction_timestamp,
        order_transaction_type
      FROM
        {{ ref('orders_line_items') }} o
      WHERE
        o.order_transaction_timestamp <= '2020-07-19'
    )

    SELECT
      * EXCEPT(row_no)
    FROM
      sorted_orders
    WHERE
      row_no = 1
  ),

  line_items AS (
    WITH sorted_line_items AS (
      SELECT
        o.order_id,
        o.order_transaction_timestamp AS line_item_transaction_timestamp,
        ROW_NUMBER() OVER (PARTITION BY line_item_id ORDER BY line_item_version DESC) AS row_num,
        l.*
      FROM
        {{ ref('orders_line_items') }} o
      CROSS JOIN
        o.line_items l
      WHERE
        o.order_transaction_timestamp <= '2020-07-19'
    )

    SELECT
      * EXCEPT(row_num)
    FROM
      sorted_line_items
    WHERE
      row_num = 1
  ),

  joined AS (
    SELECT 
      o.*,
      l.* EXCEPT(order_id)
    FROM 
      orders o
    JOIN
      line_items l
    ON
      o.order_id = l.order_id
    ORDER BY 
      o.order_id,
      o.order_version,
      o.order_transaction_timestamp,
      o.order_transaction_type
  )

  SELECT
    order_id,
    SUM(line_item_gross) AS order_gross,
    ARRAY_AGG(
      STRUCT(
        line_item_id,
        line_item_version,
        line_item_transaction_timestamp,
        line_item_product_id,
        line_item_type,
        line_item_gross,
        line_item_date
      )
    ) AS line_items
  FROM
    joined
  GROUP BY
    order_id,
    order_version,
    order_transaction_timestamp,
    order_transaction_type
