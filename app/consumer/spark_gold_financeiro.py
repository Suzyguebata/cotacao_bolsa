from pyspark.sql import SparkSession
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from minio import Minio
from consumer.tratamento import transformar_silver_para_gold_financeiro
from observability.logging_utils import configure_json_logging, log_event

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "datalake")

SILVER_PATH = os.getenv("SILVER_PATH", f"s3a://{DATA_LAKE_BUCKET}/silver/cotacoes")
GOLD_PATH = os.getenv("GOLD_FINANCIAL_PATH", f"s3a://{DATA_LAKE_BUCKET}/gold/media_precos_atualizacao_5min")
CHECKPOINT_GOLD = os.getenv("CHECKPOINT_GOLD_FINANCIAL", f"s3a://{DATA_LAKE_BUCKET}/checkpoints/gold_financeira_cotacoes")
logger = configure_json_logging("spark-gold-financial")


def wait_for_silver():
    print("Aguardando inicializacao da camada Silver...")
    log_event(logger, logging.INFO, "waiting_for_silver", silver_path=SILVER_PATH)
    client = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    while not client.bucket_exists(DATA_LAKE_BUCKET) or len(list(client.list_objects(DATA_LAKE_BUCKET, prefix="silver/cotacoes/_delta_log/"))) == 0:
        time.sleep(5)
    print("Camada Silver detectada. Iniciando Gold Financeira...")
    log_event(logger, logging.INFO, "silver_detected", silver_path=SILVER_PATH)


def build_spark_session():
    return SparkSession.builder \
        .appName("Silver_to_Gold_Financial_Aggregations") \
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
    log_event(logger, logging.INFO, "spark_gold_financial_starting", input_path=SILVER_PATH, output_path=GOLD_PATH, checkpoint_path=CHECKPOINT_GOLD)
    wait_for_silver()
    spark = build_spark_session()

    df_silver = spark.readStream \
        .format("delta") \
        .load(SILVER_PATH)

    df_gold = transformar_silver_para_gold_financeiro(df_silver)

    query = df_gold.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", CHECKPOINT_GOLD) \
        .start(GOLD_PATH)

    print(f"Processamento Gold Financeira iniciado. Gravando agregados em: {GOLD_PATH}")
    log_event(logger, logging.INFO, "spark_gold_financial_stream_started", input_path=SILVER_PATH, output_path=GOLD_PATH, checkpoint_path=CHECKPOINT_GOLD)
    query.awaitTermination()


if __name__ == "__main__":
    main()
