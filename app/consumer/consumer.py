from kafka import KafkaConsumer
from minio import Minio
import json
import pandas as pd
from datetime import datetime

consumer = KafkaConsumer(
    "cotacoes",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="grupo-local",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

minio = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

bucket = "datalake"

if not minio.bucket_exists(bucket):
    minio.make_bucket(bucket)

print("Consumer iniciado...")

for msg in consumer:
    data = msg.value["results"][0]

    df = pd.DataFrame([data])

    filename = f"cotacoes/{data['symbol']}/{datetime.now().isoformat()}.parquet"
    df.to_parquet("temp.parquet")

    minio.fput_object(bucket, filename, "temp.parquet")

    print("Arquivo salvo:", filename)
