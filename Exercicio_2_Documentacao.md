# Documentação — Exercício 2 (ETL no AWS Glue / Delta Lake)

Esta documentação descreve o ETL implementado no notebook **`ETL-hotmart-completo.ipynb`**, organizado em **Bronze → Silver → Gold**, e mapeia **explicitamente** onde cada pré-requisito do PDF é atendido.

---

## 1) Visão geral do pipeline

### Tech stack
- AWS Glue 5.0 (Spark / PySpark)
- S3 como data lake
- **Delta Lake** como formato (bronze/silver/gold)
- Athena/Glue Catalog para consulta (tabela Delta precisa estar catalogada corretamente)

### Estrutura no S3 (exemplo)
- `s3://data-lake-case-hotmart/bronze/*`
- `s3://data-lake-case-hotmart/silver/*`
- `s3://data-lake-case-hotmart/gold/*`

### Camadas
- **Bronze**: eventos CDC (append-only), com `transaction_datetime`, `transaction_date`, `ingestion_date`.
- **Silver**: dedup CDC + histórico + flag de “registro corrente” (**`Is_current_record`**).
- **Gold**: snapshots diários (`snapshot_date = D-1`) + `is_current_snapshot` + dataset final `gmv_daily_by_subsidiary`.

---

## 2) Por que Silver/Gold usam overwrite e ainda mantêm histórico

### Silver
A Silver faz **rebuild** do dataset antes de gravar:
1) lê Silver existente (se existir)
2) lê Bronze incremental (por `ingestion_date > last_ingestion`)
3) **union** (histórico + incremental)
4) dedup CDC
5) marca registro corrente
6) grava com `mode("overwrite")`

> O histórico é preservado porque ele está dentro do union+dedup, e então é regravado.

### Gold
A Gold também reconstrói antes de gravar:
1) cria o snapshot do dia (`snapshot_date`)
2) lê Gold existente (se houver)
3) **union** (histórico + snapshot do dia)
4) dedup por `(purchase_id, snapshot_date)`
5) recalcula `is_current_snapshot`
6) grava com `mode("overwrite")` particionado

> Snapshots antigos continuam existindo como partições/linhas no dataset.

---

## 3) Trechos de código (do notebook) por camada

### 3.1 Bronze (exemplo: purchase)
```python
# Bronze: adiciona ingestion_date e grava append-only em Delta (partitionBy transaction_date)
 \
    .withColumn("transaction_date", col("transaction_date").cast("date")) \
    .withColumn("purchase_id", col("purchase_id").cast("bigint")) \
    .withColumn("buyer_id", col("buyer_id").cast("bigint")) \
    .withColumn("prod_item_id", col("prod_item_id").cast("bigint")) \
    .withColumn("producer_id", col("producer_id").cast("bigint")) \
    .withColumn("order_date", col("order_date").cast("date")) \
    .withColumn("release_date", col("release_date").cast("date"))

# Controle de rastreabilidade
df_purchase_bronze = df_purchase_bronze.withColumn(
    "ingestion_date",
    to_utc_timestamp(current_timestamp(), "UTC")
)


# Persistir Bronze em DELTA (append-only)
df_purchase_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("transaction_date") \
    .save(PATH_BRONZE_PURCHASE)
```

### 3.2 Silver (exemplo: purchase)
> **Nota de nomenclatura:** no notebook aparece `is_current_record` (minúsculo).  
> Para atender seu padrão final, renomeie para **`Is_current_record`** e ajuste as referências downstream.
```python
# Silver (purchase): incremental por ingestion_date, union com silver existente, dedup CDC e flag de corrente
# CELL 2
# SILVER_PATH = "data_lake/silver/purchase"
# BRONZE_PATH = "data_lake/bronze/purchase"

# 1) Ler Silver atual (se existir)
try:
    df_silver_current = spark.read.format("delta").load(PATH_SILVER_PURCHASE)
except AnalysisException:
    df_silver_current = None

# 2) Última ingestion processada
last_ingestion = (
    df_silver_current.agg(max("bronze_ingestion_date").alias("max_date")).first()["max_date"]
) if df_silver_current is not None else None

# 3) Ler Bronze (incremental ou full)
df_bronze = spark.read.format("delta").load(PATH_BRONZE_PURCHASE)

df_bronze_incremental = (
    df_bronze.filter(col("ingestion_date") > last_ingestion)
    if last_ingestion
    else df_bronze
)

# 4) Transformações Bronze → Silver
df_bronze_ready = (
    df_bronze_incremental
    .withColumn("transaction_status", when(col("release_date").isNotNull(), "Succesfull").otherwise("Failed"))
    .withColumn("line_created_at", to_utc_timestamp(current_timestamp(), "UTC"))
    .withColumnRenamed("ingestion_date", "bronze_ingestion_date")
)

# 5) Union Silver + Bronze incremental
if df_silver_current is not None:
    cols_drop = [c for c in ["rn", "rk", "is_latest", "current_snapshot"] if c in df_silver_current.columns]
    df_union = df_silver_current.drop(*cols_drop).unionByName(df_bronze_ready)
else:
    df_union = df_bronze_ready

# 6) Deduplicação por evento (CDC) - mesma compra pode ser reenviada
event_window = Window.partitionBy("purchase_id", "transaction_datetime").orderBy(col("bronze_ingestion_date").desc())

df_silver_dedup = (
    df_union
    .withColumn("rn", row_number().over(event_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# 7) Registro corrente por purchase_id (renomeado)
purchase_window = Window.partitionBy("purchase_id").orderBy(col("transaction_datetime").desc())

df_silver_final = (
    df_silver_dedup
    .withColumn("rk", row_number().over(purchase_window))
    .withColumn("is_current_record", col("rk") == 1)   # renomeado (antes: is_latest)
    .drop("rk")
)

# 8) Escrita final (rebuild) - DELTA
df_silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .save(PATH_SILVER_PURCHASE)
```

### 3.3 Gold (core + dataset final)
> **Nota de nomenclatura:** o notebook já usa `is_current_snapshot`.  
> Se existir `current_snapshot` legado, o código remove para compatibilidade.
```python
# Gold: snapshot_date = D-1, estado as-of (<= snapshot_date), union+dedup para idempotência, is_current_snapshot e dataset final
# CELL 2
# Ler Silvers (DELTA)
df_purchase_silver = spark.read.format("delta").load(PATH_SILVER_PURCHASE)
df_product_item_silver = spark.read.format("delta").load(PATH_SILVER_PRODUCT_ITEM)
df_purchase_extra_info_silver = spark.read.format("delta").load(PATH_SILVER_EXTRA_INFO)

# snapshot_date = D-1
df_snapshot_date = spark.sql("SELECT date_sub(current_date(), 1) AS snapshot_date").collect()[0]["snapshot_date"]
print("snapshot_date (D-1):", df_snapshot_date)

# CELL 3
# -------------------------------------------------------------
# GOLD GVM (nível de compra) - foto "as-of" snapshot_date
# Para garantir reprodutibilidade e imutabilidade:
#   sempre escolhemos o último estado com transaction_date <= snapshot_date.
# -------------------------------------------------------------

# Purchase as-of
w_purchase = Window.partitionBy("purchase_id").orderBy(
    col("transaction_date").desc(),
    col("transaction_datetime").desc()
)
df_purchase_asof = (
    df_purchase_silver
    .filter(col("transaction_date") <= lit(df_snapshot_date))
    .withColumn("rn", row_number().over(w_purchase))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Product item as-of (por purchase_id + product_id)
w_item = Window.partitionBy("purchase_id","product_id").orderBy(
    col("transaction_date").desc(),
    col("transaction_datetime").desc()
)
df_item_asof = (
    df_product_item_silver
    .filter(col("transaction_date") <= lit(df_snapshot_date))
    .withColumn("rn", row_number().over(w_item))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Extra info as-of (por purchase_id)
w_extra = Window.partitionBy("purchase_id").orderBy(
    col("transaction_date").desc(),
    col("transaction_datetime").desc()
)
df_extra_asof = (
    df_purchase_extra_info_silver
    .filter(col("transaction_date") <= lit(df_snapshot_date))
    .withColumn("rn", row_number().over(w_extra))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Compras pagas (GMV considera somente release_date preenchida)
df_purchase_asof = df_purchase_asof.filter(col("transaction_status") == "Succesfull")

# Join assíncrono:
# - se item ainda não chegou, purchase_value/item_quantity ficam null e a compra não entra no GMV
# - se extra ainda não chegou, subsidiary fica null (vamos tratar como UNKNOWN na agregação)
df_new_gvm = (
    df_purchase_asof.alias("a")
    .join(df_item_asof.alias("b"), col("a.purchase_id") == col("b.purchase_id"), "left")
    .join(df_extra_asof.alias("c"), col("a.purchase_id") == col("c.purchase_id"), "left")
    .select(
        col("a.transaction_datetime"),
        col("a.purchase_id"),
        col("a.buyer_id"),
        col("a.prod_item_id"),
        col("a.order_date"),
        col("a.release_date"),
        col("a.producer_id"),
        col("b.product_id"),
        col("b.item_quantity"),
        col("b.purchase_value"),
        col("c.subsidiary"),
        current_timestamp().alias("snapshot_datetime"),
        lit(df_snapshot_date).cast("date").alias("snapshot_date"),
        col("a.transaction_date")
    )
)

df_new_gvm.show(truncate=False)

# CELL 4
# -------------------------------------------------------------
# Persistência do GOLD GVM (DELTA) - idempotente por snapshot_date
# Estratégia simples e compatível com o notebook original:
#  - lê o gold existente (se houver)
#  - union + dedup do snapshot do dia
#  - recalcula is_current_snapshot com base no max(snapshot_date)
#  - overwrite (mantendo histórico)
# -------------------------------------------------------------
try:
    df_gold_existing = spark.read.format("delta").load(PATH_GOLD)
    # compatibilidade com versões antigas: remove current_snapshot se existir
    if "current_snapshot" in df_gold_existing.columns:
        df_gold_existing = df_gold_existing.drop("current_snapshot")
    df_gold_union = df_gold_existing.unionByName(df_new_gvm, allowMissingColumns=True)
except AnalysisException:
    df_gold_union = df_new_gvm

w_snap = Window.partitionBy("purchase_id","snapshot_date").orderBy(col("snapshot_datetime").desc())
df_gold_dedup = (
    df_gold_union
    .withColumn("rn", row_number().over(w_snap))
    .filter(col("rn") == 1)
    .drop("rn")
)

max_snapshot = df_gold_dedup.select(max("snapshot_date").alias("mx")).collect()[0]["mx"]
df_gold_final = df_gold_dedup.withColumn("is_current_snapshot", col("snapshot_date") == lit(max_snapshot))

df_gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("snapshot_date","transaction_date") \
    .save(PATH_GOLD)

df_gold_final.createOrReplaceTempView("gvm_gold")
spark.sql("SELECT * FROM gvm_gold WHERE is_current_snapshot = true ORDER BY purchase_id").show(truncate=False)

# CELL 5
# -------------------------------------------------------------
# DATASET FINAL (entregável): GMV diário por subsidiária
# - regra GMV: sum(item_quantity * purchase_value)
# - particiona por transaction_date (usamos snapshot_date para garantir D-1)
# - is_current_snapshot facilita consumo (usuário não precisa de subquery de MAX)
# -------------------------------------------------------------
df_gmv_daily = spark.sql("""
    SELECT
        snapshot_date,
        release_date,
        COALESCE(subsidiary, 'UNKNOWN') AS subsidiary,
        SUM(item_quantity * purchase_value) AS gmv_value,
        COUNT(DISTINCT purchase_id) AS transaction_count,
        is_current_snapshot,
        snapshot_date AS transaction_date
    FROM gvm_gold
    GROUP BY 1,2,3,6,7
""")

df_gmv_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .save(PATH_GOLD_GMV)

df_gmv_daily.show(truncate=False)
```

### 3.4 DQ (Qualidade de Dados)
```python
# Gold DQ: quarentena de compras faturadas sem item + métricas de qualidade por snapshot
from pyspark.sql.functions import col, lit, sum as spark_sum, countDistinct, current_timestamp

DQ_PATH = "s3://data-lake-case-hotmart/gold/dq_gmv_run_metrics"
QUARANTINE_PATH = "s3://data-lake-case-hotmart/gold/quarantine_missing_items"

# Base "as-of" corrente do snapshot (mesma fonte do gvm_gold)
df_gvm_snapshot = spark.table("gvm_gold").where(col("snapshot_date") == lit(df_snapshot_date))

# 1) Quarentena: compras faturadas sem item (não entram no GMV)
df_quarantine_missing_item = (
    df_gvm_snapshot
    .where(col("release_date").isNotNull())
    .where(col("product_id").isNull())  # sem item
    .select(
        "snapshot_date",
        "purchase_id",
        "buyer_id",
        "prod_item_id",
        "order_date",
        "release_date",
        "producer_id",
        "subsidiary",
        "transaction_date",
        "snapshot_datetime"
    )
)

df_quarantine_missing_item.write.format("delta") \
    .mode("append") \
    .partitionBy("snapshot_date") \
    .save(QUARANTINE_PATH)

# 2) Métricas DQ do run
df_dq_metrics = (
    df_gvm_snapshot.agg(
        countDistinct("purchase_id").alias("total_purchases_asof"),
        countDistinct(F.when(col("release_date").isNotNull(), col("purchase_id"))).alias("succeded_transactions"),
        F.sum(F.when(col("product_id").isNull() & col("release_date").isNotNull(), 1).otherwise(0)).alias("missing_product_item_cnt"),
        F.sum(F.when(col("subsidiary").isNull(), 1).otherwise(0)).alias("missing_extra_info_cnt"),
        F.sum(F.when(col("subsidiary") == "UNKNOWN", 1).otherwise(0)).alias("unknown_subsidiary_cnt"),
        F.count("*").alias("gvm_rows"),
        spark_sum(col("item_quantity") * col("purchase_value")).alias("gmv_total_value")
    )
    .withColumn("snapshot_date", lit(df_snapshot_date).cast("date"))
    .withColumn("snapshot_datetime", current_timestamp())
)

df_dq_metrics.write.format("delta") \
    .mode("append") \
    .partitionBy("snapshot_date") \
    .save(DQ_PATH)
```

---

## 4) Pré-requisitos do PDF — mapeamento explícito para o código

Abaixo, cada item do PDF com o “onde no código” (por bloco) e o porquê.

### (1) Modelagem baseada em eventos CDC
- **Bronze** grava eventos append-only com `transaction_datetime`/`transaction_date`.
- **Silver** deduplica eventos (reenvio) por chave de evento e `bronze_ingestion_date`.

**Onde:** Bronze (purchase/product_item/extra_info) + Silver (dedup CDC).  
**Trecho:** veja Bronze (ingestion_date + append) e Silver (event_window + row_number).

---

### (2) Dados podem chegar inconsistentes / incompletos
- **Quarentena**: compra faturada sem item não entra no GMV e é registrada em `gold/quarantine_missing_items`.
- **Métricas DQ**: tabela `gold/dq_gmv_run_metrics` com contagens e `gmv_total_value`.

**Onde:** bloco DQ no final do notebook (`df_quarantine_missing_item` e `df_dq_metrics`).  

---

### (3) Todas as tabelas são gatilhos
- Gold sempre lê as três Silvers (purchase, product_item, extra_info) para construir o snapshot.

**Onde:** início do bloco Gold (`spark.read.format("delta").load(PATH_SILVER_...)`).  

---

### (4) Se só uma tabela mudar, repetir ativos das outras
- Gold constrói **estado as-of** `snapshot_date` para **cada Silver** usando:
  - `filter(transaction_date <= snapshot_date)` + `row_number` ordenado desc.

Isso “carrega para frente” (carry-forward) os registros correntes das tabelas que não mudaram no dia.

**Onde:** blocos “Purchase as-of”, “Product item as-of” e “Extra info as-of” no Gold.

---

### (5) Atualização em D-1
- `snapshot_date = date_sub(current_date(), 1)`.

**Onde:** Gold, linha do `df_snapshot_date`.

---

### (6) Reprocessamento full não altera o passado (imutabilidade)
- O snapshot do dia é identificado por `snapshot_date`.
- O Gold faz `union(existing + new_snapshot)` e dedup por `(purchase_id, snapshot_date)` antes de regravar.

Isso impede que rodar novamente “mude” snapshots anteriores.

**Onde:** Gold, bloco “Persistência do GOLD GVM”.

---

### (7) Navegação temporal (Jan/2023 visto em 31/03/2023 vs hoje)
- Cada `snapshot_date` representa a “visão do mundo” naquele dia.
- Para as-of, você consulta o snapshot mais recente `<= data referência`.
- Para hoje, `is_current_snapshot = true`.

**Onde:** `is_current_snapshot` e partições por `snapshot_date`.

---

### (8) Rastreabilidade em granularidade diária (não só datetime)
- `transaction_date` e `snapshot_date` são colunas do dia.
- A pipeline opera com “as-of” por `transaction_date <= snapshot_date` (diário).

**Onde:** filtros `filter(col("transaction_date") <= lit(df_snapshot_date))` no Gold.

---

### (9) Partição por transaction_date
- Bronze e Silver gravam `partitionBy("transaction_date")`.
- Dataset final usa `transaction_date = snapshot_date` e particiona por `transaction_date`.

**Onde:** writes de Bronze/Silver/Gold.

---

### (10) Recuperar registros correntes facilmente
- Silver: `Is_current_record` (registro corrente por PK)
- Gold: `is_current_snapshot` (snapshot corrente)

**Onde:** Silver (coluna de corrente) + Gold (`is_current_snapshot`).

---

### (11) SQL final (GMV diário por subsidiária, corrente)
Dataset final: `gold/gmv_daily_by_subsidiary` (Delta).

Query:
```sql
SELECT
  release_date,
  subsidiary,
  gmv_value,
  transaction_count
FROM gold_gmv_daily_by_subsidiary
WHERE is_current_snapshot = TRUE
ORDER BY release_date, subsidiary;
```

---

## 5) Entregáveis (Exercício 2)

1) **ETL em Glue**: notebook `ETL-hotmart-completo.ipynb` (Bronze/Silver/Gold + DQ).  
2) **Dataset final**: `s3://data-lake-case-hotmart/gold/gmv_daily_by_subsidiary` (Delta).  
3) **Tabela de quarentena**: `s3://data-lake-case-hotmart/gold/quarantine_missing_items` (Delta).  
4) **Métricas DQ**: `s3://data-lake-case-hotmart/gold/dq_gmv_run_metrics` (Delta).  
5) **SQL de consumo**: query acima (corrente) + as-of (via snapshot_date).
