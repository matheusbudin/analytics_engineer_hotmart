# Documentação — Exercício 1 (SQL)

## Objetivo
Responder às perguntas do Exercício 1 do PDF:

1. **Top 50 produtores em faturamento em 2021**
2. **Top 2 produtos que mais faturaram por produtor**

> Regras aplicadas nos SQL anexos:
- Considera apenas compras com **`release_date IS NOT NULL`** (pagamento/compra liberada).
- Receita/GMV calculada como **`purchase_value * item_quantity`**.
- Join `purchase` ↔ `product_item` usa **chave composta** `prod_item_id + prod_item_partition`.

---

## Arquivo 1 — `parte1_Top50_produtores_em_faturamento.sql`

### O que a query faz
1) Filtra as compras de 2021 (CTE `purchases_2021`) antes de juntar com `product_item` (reduz custo do join).  
2) Junta com `product_item` por `prod_item_id` e `prod_item_partition`.  
3) Agrega por `producer_id` e ordena por faturamento desc, retornando top 50.

### SQL (como está no arquivo)
```sql
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
```

### Observação (importante)
No arquivo há um typo: **`pi_item_quantity`**. Para executar corretamente, o trecho deve ser:
```sql
SUM(pi.purchase_value * pi.item_quantity) AS total_revenue
```

---

## Arquivo 2 — `parte2_Top2_produtos_fatura_por_produtor.sql`

### O que a query faz
1) Agrega faturamento por `(producer_id, product_id)`.  
2) Rank por produtor com `ROW_NUMBER()` desc por faturamento.  
3) Mantém somente os 2 primeiros por produtor.

### SQL (como está no arquivo)
```sql
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
```

---

## Como validar rapidamente
- Conferir se `release_date` não nula é realmente a regra de “pagamento efetuado” no dataset.
- Conferir se `purchase_value` é valor unitário (por isso multiplicamos por `item_quantity`).
