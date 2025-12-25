#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Silver Layer uniquement : Bronze -> Delta Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

print("🚀 Initialisation de Spark - Silver Layer...")

# Initialiser Spark avec support Delta
spark = SparkSession.builder \
    .appName("SilverLayer-BronzeAggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/delta/checkpoints") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session créée avec succès")

print("\n" + "=" * 80)
print("FLUX SILVER : Bronze → Delta Silver (Agrégations)")
print("=" * 80)

# Vérifier que la table Bronze existe
bronze_path = "/tmp/delta/bronze/ventes_stream"
print(f"🔍 Vérification de la table Bronze : {bronze_path}")

max_retries = 10
retry_count = 0

while retry_count < max_retries:
    try:
        test_df = spark.read.format("delta").load(bronze_path)
        bronze_count = test_df.count()
        print(f"✅ Table Bronze trouvée avec {bronze_count} enregistrements")
        break
    except Exception as e:
        retry_count += 1
        print(f"⏳ Tentative {retry_count}/{max_retries} - Table Bronze pas encore prête...")
        if retry_count == max_retries:
            print("❌ Erreur : La table Bronze n'existe pas encore.")
            print("   Veuillez d'abord démarrer le flux Bronze (spark_streaming_bronze.py)")
            spark.stop()
            exit(1)
        time.sleep(5)

# Lecture du flux Bronze
print("\n📖 Lecture du flux Bronze en streaming...")
df_silver_stream = spark.readStream \
    .format("delta") \
    .load(bronze_path)

print("✅ Lecture du flux Bronze établie")

# Application du Watermark pour gérer les données tardives
df_silver_watermarked = df_silver_stream.withWatermark("timestamp_event", "10 minutes")

print("✅ Watermark appliqué (10 minutes)")

# Agrégation des données par fenêtre de temps et dimensions
df_silver_agg = df_silver_watermarked.groupBy(
    F.window(F.col("timestamp_event"), "1 minute", "1 minute").alias("window"),
    "client_id",
    "client_nom",
    "pays",
    "segment"
).agg(
    F.sum("quantite").alias("total_quantite"),
    F.sum("montant").alias("total_depense"),
    F.count("*").alias("nb_achats"),
    F.avg("montant").alias("panier_moyen")
).withColumn("est_client_fidele", F.when(F.col("nb_achats") >= 2, True).otherwise(False)) \
 .withColumn("window_start", F.col("window.start")) \
 .withColumn("window_end", F.col("window.end")) \
 .drop("window")

print("✅ Agrégations configurées (fenêtres de 1 minute)")

# Écriture en Delta Lake Silver
query_silver = df_silver_agg.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/ventes_silver") \
    .start("/tmp/delta/silver/ventes_aggreges")

print("✅ Streaming Silver démarré")

print("\n" + "=" * 80)
print("📊 SILVER LAYER ACTIVE")
print("=" * 80)
print("Silver Layer : /tmp/delta/silver/ventes_aggreges")
print("\n⏳ Flux actif. Appuyez sur Ctrl+C pour arrêter...")
print("=" * 80)

# Attendre la fin du flux
try:
    query_silver.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹️  Arrêt du flux Silver...")
    query_silver.stop()
    print("✅ Flux Silver arrêté proprement")
finally:
    spark.stop()
    print("✅ Spark Session fermée")