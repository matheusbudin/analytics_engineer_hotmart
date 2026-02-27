
--=========================================================
-- 50 maiores produtores em faturamento ($) de 2021
--=========================================================
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
    SUM(pi.purchase_value * pi.item_quantity) AS total_revenue
FROM purchases_2021 p
JOIN product_item pi
  ON pi.prod_item_id = p.prod_item_id
 AND pi.prod_item_partition = p.prod_item_partition
GROUP BY p.producer_id
ORDER BY total_revenue DESC
LIMIT 50;


--=========================================================
-- 2 produtos que mais faturaram ($) de cada produtor
--=========================================================
/*
filtro antes de fazer o join: se for tabela pequena CTE, se for grande temp_tables
ganho em performance de processamento
Parte 1: Top 50 produtores em FATURAMENTO em 2021
*/
WITH revenue_by_producer_product AS (
    SELECT
        p.producer_id,
        pi.product_id,
        SUM(pi.purchase_value * pi.item_quantity) AS product_revenue
    FROM purchase p
    JOIN product_item pi
      ON pi.prod_item_id = p.prod_item_id
     AND pi.prod_item_partition = p.prod_item_partition
    WHERE p.release_date IS NOT NULL
    GROUP BY p.producer_id, pi.product_id
),
ranked AS (
    SELECT
        producer_id,
        product_id,
        product_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY producer_id
            ORDER BY product_revenue DESC
        ) AS rn
    FROM revenue_by_producer_product
)
SELECT
    producer_id,
    product_id,
    product_revenue
FROM ranked
WHERE rn <= 2
ORDER BY producer_id, product_revenue DESC;