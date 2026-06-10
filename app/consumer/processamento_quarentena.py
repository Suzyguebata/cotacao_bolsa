from pyspark.sql import SparkSession
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from minio import Minio
from consumer.tratamento import transformar_bronze_para_quarentena
from observability.logging_utils import configure_json_logging, log_event

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "datalake")

BRONZE_PATH = os.getenv("BRONZE_PATH", f"s3a://{DATA_LAKE_BUCKET}/bronze/cotacoes")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", f"s3a://{DATA_LAKE_BUCKET}/silver/cotacoes_rejeitadas")
CHECKPOINT_QUARANTINE = os.getenv("CHECKPOINT_QUARANTINE", f"s3a://{DATA_LAKE_BUCKET}/checkpoints/silver_cotacoes_rejeitadas")
logger = configure_json_logging("spark-quarentena")


def aguardar_bronze():
    print("Aguardando inicializacao da camada Bronze...")
    log_event(logger, logging.INFO, "waiting_for_bronze", caminho_bronze=BRONZE_PATH)
    cliente = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    while not cliente.bucket_exists(DATA_LAKE_BUCKET) or len(list(cliente.list_objects(DATA_LAKE_BUCKET, prefix="bronze/cotacoes/_delta_log/"))) == 0:
        time.sleep(5)
    print("Camada Bronze detectada. Iniciando quarentena...")
    log_event(logger, logging.INFO, "bronze_detected", caminho_bronze=BRONZE_PATH)


def criar_sessao_spark():
    return SparkSession.builder \
        .appName("Rejeita_cotas_quarentena_camada_bronze") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT_URL}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def main():
    log_event(logger, logging.INFO, "spark_quarantine_starting", caminho_entrada=BRONZE_PATH, caminho_saida=QUARANTINE_PATH, caminho_checkpoint=CHECKPOINT_QUARANTINE)
    aguardar_bronze()
    spark = criar_sessao_spark()

    df_bronze = spark.readStream \
        .format("delta") \
        .load(BRONZE_PATH)

    df_quarentena = transformar_bronze_para_quarentena(df_bronze)

    query = df_quarentena.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_QUARANTINE) \
        .start(QUARANTINE_PATH)

    print(f"Quarentena iniciada. Gravando rejeitados em: {QUARANTINE_PATH}")
    log_event(logger, logging.INFO, "spark_quarantine_stream_started", caminho_entrada=BRONZE_PATH, caminho_saida=QUARANTINE_PATH, caminho_checkpoint=CHECKPOINT_QUARANTINE)
    query.awaitTermination()


if __name__ == "__main__":
    main()
