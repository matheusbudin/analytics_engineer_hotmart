import os
import boto3
from botocore.exceptions import ClientError

def download_bucket(bucket_name: str, local_dir: str, prefix: str = "") -> None:
    """
    Baixa todos os objetos de um bucket S3 (opcionalmente filtrando por prefix),
    preservando a estrutura de pastas localmente.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    total = 0
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # pula "marcadores" de pasta
            if key.endswith("/"):
                continue

            local_path = os.path.join(local_dir, key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            try:
                s3.download_file(bucket_name, key, local_path)
                total += 1
                print(f"[OK] s3://{bucket_name}/{key} -> {local_path}")
            except ClientError as e:
                print(f"[ERRO] Falha ao baixar {key}: {e}")

    print(f"\nConcluído. Arquivos baixados: {total}")

if __name__ == "__main__":
    BUCKET = "data-lake-case-hotmart"
    LOCAL_DIR = "./data-lake-case-hotmart" 
    PREFIX = ""  

    download_bucket(BUCKET, LOCAL_DIR, PREFIX)