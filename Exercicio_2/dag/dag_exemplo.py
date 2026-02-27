# dags/hotmart_case_etl.py
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# Se estiver em MWAA / Airflow com provider AWS:
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# ========= CONFIG (ajuste para seu ambiente) =========
AWS_CONN_ID = "aws_default"
BUCKET = "data-lake-case-hotmart"

# Prefixos onde a Bronze escreve (Delta/Parquet, tanto faz para o check)
# Exemplo: s3://data-lake-case-hotmart/data_lake/bronze/purchase/transaction_date=YYYY-MM-DD/...
BRONZE_PREFIX_BY_TABLE = {
    "purchase": "data_lake/bronze/purchase",
    "product_item": "data_lake/bronze/product_item",
    "purchase_extra_info": "data_lake/bronze/purchase_extra_info",
}

# Nomes dos Glue Jobs (um por notebook/camada). Ajuste para os seus.
GLUE_JOBS = {
    "bronze_purchase": "bronze_purchase_job",
    "bronze_product_item": "bronze_product_item_job",
    "bronze_purchase_extra_info": "bronze_purchase_extra_info_job",
    "silver_purchase": "silver_purchase_job",
    "silver_product_item": "silver_product_item_job",
    "silver_purchase_extra_info": "silver_purchase_extra_info_job",
    "gold_gvm": "gold_gvm_job",
}

# =====================================================


def _has_bronze_partition(table: str, snapshot_date: str) -> bool:
    """
    Retorna True se existir pelo menos 1 objeto no prefixo da partição de D-1.
    Isso é o gatilho "chegou dado novo na Bronze para D-1?".
    """
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    base_prefix = BRONZE_PREFIX_BY_TABLE[table].rstrip("/")
    # padrão Hive partition: transaction_date=YYYY-MM-DD
    partition_prefix = f"{base_prefix}/transaction_date={snapshot_date}/"
    return s3.check_for_prefix(bucket_name=BUCKET, prefix=partition_prefix, delimiter="/")


def _skip_if_no_bronze_data(table: str, snapshot_date: str, **context) -> None:
    """
    Se não houver dados na Bronze para snapshot_date (D-1),
    a Silver deve ser considerada 'up-to-date' e pode ser SKIPPED.
    """
    if not _has_bronze_partition(table=table, snapshot_date=snapshot_date):
        raise AirflowSkipException(
            f"[{table}] Sem dados novos na Bronze para {snapshot_date}. "
            "Pulando Silver (up-to-date via carry-forward)."
        )


with DAG(
    dag_id="hotmart_case_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule="0 6 * * *",  # todo dia 06:00
    catchup=False,
    max_active_runs=1,
    tags=["hotmart", "case", "etl", "bronze-silver-gold"],
) as dag:

    # Airflow "ds" é o logical date (YYYY-MM-DD).
    # Para D-1, usamos macros.ds_add(ds, -1)
    SNAPSHOT_DATE = "{{ macros.ds_add(ds, -1) }}"

    start = EmptyOperator(task_id="start")

    # ======================
    # 1) BRONZE (paralelo)
    # ======================
    bronze_purchase = AwsGlueJobOperator(
        task_id="bronze_purchase",
        job_name=GLUE_JOBS["bronze_purchase"],
        aws_conn_id=AWS_CONN_ID,
        # Se você passar parâmetros pro job:
        # script_args={"--RUN_DATE": SNAPSHOT_DATE}
    )

    bronze_product_item = AwsGlueJobOperator(
        task_id="bronze_product_item",
        job_name=GLUE_JOBS["bronze_product_item"],
        aws_conn_id=AWS_CONN_ID,
    )

    bronze_purchase_extra_info = AwsGlueJobOperator(
        task_id="bronze_purchase_extra_info",
        job_name=GLUE_JOBS["bronze_purchase_extra_info"],
        aws_conn_id=AWS_CONN_ID,
    )

    # ======================
    # 2) SILVER (cada uma depende da Bronze correspondente)
    #    - se não chegou dado novo em D-1, pula (SKIP) e fica "up-to-date"
    # ======================
    # purchase -> silver_purchase
    check_bronze_purchase_d1 = PythonOperator(
        task_id="check_bronze_purchase_has_d1",
        python_callable=_skip_if_no_bronze_data,
        op_kwargs={"table": "purchase", "snapshot_date": SNAPSHOT_DATE},
    )
    silver_purchase = AwsGlueJobOperator(
        task_id="silver_purchase",
        job_name=GLUE_JOBS["silver_purchase"],
        aws_conn_id=AWS_CONN_ID,
        # script_args={"--SNAPSHOT_DATE": SNAPSHOT_DATE}
    )

    # product_item -> silver_product_item
    check_bronze_product_item_d1 = PythonOperator(
        task_id="check_bronze_product_item_has_d1",
        python_callable=_skip_if_no_bronze_data,
        op_kwargs={"table": "product_item", "snapshot_date": SNAPSHOT_DATE},
    )
    silver_product_item = AwsGlueJobOperator(
        task_id="silver_product_item",
        job_name=GLUE_JOBS["silver_product_item"],
        aws_conn_id=AWS_CONN_ID,
    )

    # purchase_extra_info -> silver_purchase_extra_info
    check_bronze_extra_d1 = PythonOperator(
        task_id="check_bronze_purchase_extra_info_has_d1",
        python_callable=_skip_if_no_bronze_data,
        op_kwargs={"table": "purchase_extra_info", "snapshot_date": SNAPSHOT_DATE},
    )
    silver_purchase_extra_info = AwsGlueJobOperator(
        task_id="silver_purchase_extra_info",
        job_name=GLUE_JOBS["silver_purchase_extra_info"],
        aws_conn_id=AWS_CONN_ID,
    )

    # ======================
    # 3) GOLD (só quando todas as Silvers estão "up-to-date")
    #    - Up-to-date = SUCCESS ou SKIPPED
    #    - Falha em qualquer silver bloqueia o Gold
    # ======================
    gold_gvm = AwsGlueJobOperator(
        task_id="gold_gvm",
        job_name=GLUE_JOBS["gold_gvm"],
        aws_conn_id=AWS_CONN_ID,
        # O ouro deve usar SNAPSHOT_DATE (D-1) e carry-forward (as-of)
        # script_args={"--SNAPSHOT_DATE": SNAPSHOT_DATE}
        trigger_rule=TriggerRule.NONE_FAILED,  # roda mesmo se upstream foi SKIPPED
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    # ======================
    # DEPENDÊNCIAS
    # ======================
    start >> [bronze_purchase, bronze_product_item, bronze_purchase_extra_info]

    bronze_purchase >> check_bronze_purchase_d1 >> silver_purchase
    bronze_product_item >> check_bronze_product_item_d1 >> silver_product_item
    bronze_purchase_extra_info >> check_bronze_extra_d1 >> silver_purchase_extra_info

    # Gold só depois que TODAS as silvers concluíram (success/skip)
    [silver_purchase, silver_product_item, silver_purchase_extra_info] >> gold_gvm >> end