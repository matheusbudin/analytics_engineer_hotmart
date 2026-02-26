/*
filtro antes de fazer o join: se for tabela pequena CTE, se for grande temp_tables
ganho em performance de processamento
Parte 1: Top 50 produtores em FATURAMENTO em 2021
*/
WITH purchases_2021 AS (
    SELECT
        purchase_id,
        producer_id,
        prod_item_id,
        prod_item_partition
    FROM purchase
    WHERE year(order_date) = 2021
      AND release_date IS NOT NULL
)
SELECT
    p.producer_id,
    SUM(pi.purchase_value * pi_item_quantity) AS total_revenue
FROM purchases_2021 p
JOIN product_item pi
  ON pi.prod_item_id = p.prod_item_id
 AND pi.prod_item_partition = p.prod_item_partition
GROUP BY p.producer_id
ORDER BY total_revenue DESC
LIMIT 50;