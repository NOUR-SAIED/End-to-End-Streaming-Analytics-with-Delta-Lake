#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Bronze Layer uniquement : Kafka -> Delta Bronze
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("🚀 Initialisation de Spark - Bronze Layer...")

# Initialiser Spark avec support Delta
spark = SparkSession.builder \
    .appName("BronzeLayer-KafkaToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/delta/checkpoints") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session créée avec succès")

# Schéma des données Kafka
schema = StructType([
    StructField("vente_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("produit_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("quantite", IntegerType(), True),
    StructField("montant", DoubleType(), True),
    StructField("client_nom", StringType(), True),
    StructField("produit_nom", StringType(), True),
    StructField("categorie", StringType(), True),
    StructField("pays", StringType(), True),
    StructField("segment", StringType(), True)
])

print("\n" + "=" * 80)
print("FLUX BRONZE : Kafka → Delta Bronze")
print("=" * 80)

# Lecture du flux Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ventes_stream") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✅ Connexion Kafka établie")

# Parsing des données JSON
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Enrichissement des données Bronze
df_bronze_enriched = df_parsed \
    .withColumn("timestamp_event", to_timestamp(col("timestamp"))) \
    .withColumn("date_ingestion", current_timestamp()) \
    .withColumn("jour", substring(col("timestamp"), 1, 10))

print("✅ Schéma appliqué et enrichissement effectué")

# Écriture en Delta Lake Bronze
query_bronze = df_bronze_enriched.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/ventes_bronze") \
    .partitionBy("jour") \
    .start("/tmp/delta/bronze/ventes_stream")

print("✅ Streaming Bronze démarré")
print("\n" + "=" * 80)
print("📊 BRONZE LAYER ACTIVE")
print("=" * 80)
print("Bronze Layer : /tmp/delta/bronze/ventes_stream")
print("\n⏳ Flux actif. Appuyez sur Ctrl+C pour arrêter...")
print("=" * 80)

# Attendre la fin du flux
try:
    query_bronze.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹️  Arrêt du flux Bronze...")
    query_bronze.stop()
    print("✅ Flux Bronze arrêté proprement")
finally:
    spark.stop()
    print("✅ Spark Session fermée")