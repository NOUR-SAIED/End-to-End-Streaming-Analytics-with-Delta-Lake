#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script pour créer les tables Delta vides AVANT de lancer les flux streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

print("🚀 Création des tables Delta vides...")

# Créer Spark Session
spark = SparkSession.builder \
    .appName("CreateDeltaTables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schéma Bronze (identique au producteur + colonnes ajoutées)
schema_bronze = StructType([
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
    StructField("segment", StringType(), True),
    StructField("timestamp_event", TimestampType(), True),
    StructField("date_ingestion", TimestampType(), True),
    StructField("jour", StringType(), True)
])

# Schéma Silver
schema_silver = StructType([
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("client_nom", StringType(), True),
    StructField("pays", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("total_quantite", LongType(), True),
    StructField("total_depense", DoubleType(), True),
    StructField("nb_achats", LongType(), True),
    StructField("panier_moyen", DoubleType(), True),
    StructField("est_client_fidele", BooleanType(), True)
])

# Créer DataFrame vide Bronze
print("\n📊 Création de la table Bronze...")
df_bronze_empty = spark.createDataFrame([], schema_bronze)
df_bronze_empty.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("jour") \
    .save("/tmp/delta/bronze/ventes_stream")
print("✅ Table Bronze créée : /tmp/delta/bronze/ventes_stream")

# Créer DataFrame vide Silver
print("\n📊 Création de la table Silver...")
df_silver_empty = spark.createDataFrame([], schema_silver)
df_silver_empty.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/tmp/delta/silver/ventes_aggreges")
print("✅ Table Silver créée : /tmp/delta/silver/ventes_aggreges")

print("\n" + "=" * 80)
print("✅ TOUTES LES TABLES DELTA SONT CRÉÉES !")
print("=" * 80)
print("\nVous pouvez maintenant lancer :")
print("1. Le producteur")
print("2. Le flux Bronze")
print("3. Le flux Silver")
print("=" * 80)

spark.stop()