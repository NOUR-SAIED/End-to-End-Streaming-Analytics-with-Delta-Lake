#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
from pyspark.sql import SparkSession

print("🚀 Démarrage du Dashboard Temps Réel (Partie 5)")
print("📊 Analyses métier enrichies + Supervision (Partie 6)")
print("=" * 70)

# Initialisation Spark avec support Delta
spark = SparkSession.builder \
    .appName("DashboardQuery") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session créée\n")

try:
    while True:
        timestamp = time.strftime('%H:%M:%S')
        print(f"\n[{timestamp}] 🔍 Rafraîchissement des données depuis Delta Silver...")
        
        try:
            # 🔹 Lecture de la table Silver
            df = spark.read.format("delta").load("/tmp/delta/silver/ventes_aggreges")
            df.createOrReplaceTempView("ventes_silver")

            # ====== PARTIE 5 : REQUÊTE EXIGÉE DANS L'ÉNONCÉ ======
            print("\n1️⃣ TOP CA par pays & segment")
            print("-" * 50)
            top_ca = spark.sql("""
                SELECT pays, segment, SUM(total_depense) as ca_total
                FROM ventes_silver
                GROUP BY pays, segment
                ORDER BY ca_total DESC
                LIMIT 10
            """)
            top_ca.show(truncate=False)

            # ====== ANALYSES ENRICHIES (au-delà du minimum) ======
            print("\n2️⃣ ÉVOLUTION HORAIRE (dernières 24h)")
            print("-" * 50)
            trend = spark.sql("""
                SELECT 
                    window_start,
                    SUM(total_depense) AS ca_horaire,
                    SUM(nb_achats) AS ventes_horaire
                FROM ventes_silver
                WHERE window_start >= current_timestamp() - INTERVAL 24 HOURS
                GROUP BY window_start
                ORDER BY window_start
            """)
            trend.show(truncate=False)

            print("\n3️⃣ TAUX DE FIDÉLITÉ par segment")
            print("-" * 50)
            fidelite = spark.sql("""
                SELECT 
                    segment,
                    COUNT(*) AS nb_fenetres,
                    SUM(CASE WHEN est_client_fidele THEN 1 ELSE 0 END) AS nb_fenetres_fideles,
                    ROUND(100.0 * SUM(CASE WHEN est_client_fidele THEN 1 ELSE 0 END) / COUNT(*), 2) AS taux_fidelite_pct
                FROM ventes_silver
                GROUP BY segment
                ORDER BY taux_fidelite_pct DESC
            """)
            fidelite.show(truncate=False)

            print("\n4️⃣ CLIENTS 'MÉTÉORES' (1 achat > 500€)")
            print("-" * 50)
            meteores = spark.sql("""
                SELECT 
                    client_nom,
                    pays,
                    MAX(total_depense) AS max_vente,
                    SUM(nb_achats) AS total_achats
                FROM ventes_silver
                GROUP BY client_nom, pays
                HAVING SUM(nb_achats) = 1 AND MAX(total_depense) > 500
                ORDER BY max_vente DESC
                LIMIT 5
            """)
            meteores.show(truncate=False)

            print("\n5️⃣ STATISTIQUES GLOBALES (Supervision - Partie 6)")
            print("-" * 50)
            stats = spark.sql("""
                SELECT 
                    COUNT(*) as nb_fenetres,
                    SUM(nb_achats) as total_ventes,
                    ROUND(SUM(total_depense), 2) as ca_total,
                    ROUND(AVG(panier_moyen), 2) as panier_moyen
                FROM ventes_silver
            """)
            stats.show(truncate=False)

            print(f"\n✅ Mise à jour terminée à {timestamp}")
            print("=" * 70)

        except Exception as e:
            print(f"❌ Erreur lors du rafraîchissement : {e}")
            print("   → Vérifiez que le flux Silver est actif et que la table existe.")

        print("⏳ Prochain rafraîchissement dans 30 secondes... (Ctrl+C pour arrêter)")
        time.sleep(30)

except KeyboardInterrupt:
    print("\n⏹️  Arrêt demandé par l'utilisateur.")

finally:
    spark.stop()
    print("✅ Spark Session fermée.")