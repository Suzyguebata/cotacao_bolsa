from pyspark.sql import SparkSession
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from minio import Minio
from consumer.tratamento import transformar_bronze_para_silver
from observability.logging_utils import configure_json_logging, log_event

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "datalake")

BRONZE_PATH = os.getenv("BRONZE_PATH", f"s3a://{DATA_LAKE_BUCKET}/bronze/cotacoes")
SILVER_PATH = os.getenv("SILVER_PATH", f"s3a://{DATA_LAKE_BUCKET}/silver/cotacoes")
CHECKPOINT_SILVER = os.getenv("CHECKPOINT_SILVER", f"s3a://{DATA_LAKE_BUCKET}/checkpoints/silver_cotacoes")
logger = configure_json_logging("spark-silver")


def aguardar_bronze():
    print("Aguardando inicializacao da camada Bronze...")
    log_event(logger, logging.INFO, "waiting_for_bronze", caminho_bronze=BRONZE_PATH)
    cliente = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    while not cliente.bucket_exists(DATA_LAKE_BUCKET) or len(list(cliente.list_objects(DATA_LAKE_BUCKET, prefix="bronze/cotacoes/_delta_log/"))) == 0:
        time.sleep(5)
    print("Camada Bronze detectada. Iniciando Silver...")
    log_event(logger, logging.INFO, "bronze_detected", caminho_bronze=BRONZE_PATH)


def criar_sessao_spark():
    return SparkSession.builder \
        .appName("Bronze_to_Silver_Refinement") \
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
    log_event(logger, logging.INFO, "spark_silver_starting", caminho_entrada=BRONZE_PATH, caminho_saida=SILVER_PATH, caminho_checkpoint=CHECKPOINT_SILVER)
    aguardar_bronze()
    spark = criar_sessao_spark()

    df_bronze = spark.readStream \
        .format("delta") \
        .load(BRONZE_PATH)

    df_silver = transformar_bronze_para_silver(df_bronze)

    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("ticket_ativo_b3", "ano_mes_dia") \
        .trigger(processingTime='5 minutes') \
        .option("checkpointLocation", CHECKPOINT_SILVER) \
        .start(SILVER_PATH)

    print(f"Refinamento Silver iniciado. Gravando em: {SILVER_PATH}")
    log_event(logger, logging.INFO, "spark_silver_stream_started", caminho_entrada=BRONZE_PATH, caminho_saida=SILVER_PATH, caminho_checkpoint=CHECKPOINT_SILVER)
    query.awaitTermination()


if __name__ == "__main__":
    main()
