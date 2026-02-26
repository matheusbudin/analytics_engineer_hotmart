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