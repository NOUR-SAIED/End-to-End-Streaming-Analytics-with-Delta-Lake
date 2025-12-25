#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuration Kafka
KAFKA_SERVER = "kafka:9092"
TOPIC_NAME = "ventes_stream"

# Données de simulation
PRODUITS = [
    {"id": 1, "nom": "Laptop Pro", "categorie": "Électronique"},
    {"id": 2, "nom": "Smartphone X", "categorie": "Électronique"},
    {"id": 3, "nom": "Chaise Bureau", "categorie": "Mobilier"},
    {"id": 4, "nom": "Bureau Standing", "categorie": "Mobilier"},
    {"id": 5, "nom": "Casque Audio", "categorie": "Accessoires"},
    {"id": 6, "nom": "Souris Gaming", "categorie": "Accessoires"},
    {"id": 7, "nom": "Clavier Mécanique", "categorie": "Accessoires"},
    {"id": 8, "nom": "Écran 4K", "categorie": "Électronique"}
]

CLIENTS = [
    {"id": 1, "nom": "Alice Martin", "pays": "France", "segment": "Premium"},
    {"id": 2, "nom": "Bob Dupont", "pays": "France", "segment": "Standard"},
    {"id": 3, "nom": "Charlie Smith", "pays": "UK", "segment": "Premium"},
    {"id": 4, "nom": "Diana Johnson", "pays": "USA", "segment": "Premium"},
    {"id": 5, "nom": "Eve Brown", "pays": "USA", "segment": "Standard"},
    {"id": 6, "nom": "Frank Wilson", "pays": "Germany", "segment": "Premium"},
    {"id": 7, "nom": "Grace Taylor", "pays": "Canada", "segment": "Standard"},
    {"id": 8, "nom": "Henry Davis", "pays": "Spain", "segment": "Premium"}
]

def create_producer():
    """Crée et retourne un producteur Kafka"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1)
            )
            print(f"✅ Connecté à Kafka sur {KAFKA_SERVER}")
            return producer
        except Exception as e:
            retry_count += 1
            print(f"⚠️ Tentative {retry_count}/{max_retries} - Erreur: {e}")
            time.sleep(5)
    
    raise Exception("❌ Impossible de se connecter à Kafka après plusieurs tentatives")

def generate_vente(vente_id):
    """Génère une transaction de vente aléatoire"""
    client = random.choice(CLIENTS)
    produit = random.choice(PRODUITS)
    quantite = random.randint(1, 5)
    
    # Prix de base selon la catégorie
    prix_base = {
        "Électronique": random.randint(300, 2000),
        "Mobilier": random.randint(150, 800),
        "Accessoires": random.randint(30, 200)
    }
    
    montant = prix_base[produit["categorie"]] * quantite
    
    vente = {
        "vente_id": vente_id,
        "client_id": client["id"],
        "produit_id": produit["id"],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "quantite": quantite,
        "montant": float(montant),
        "client_nom": client["nom"],
        "produit_nom": produit["nom"],
        "categorie": produit["categorie"],
        "pays": client["pays"],
        "segment": client["segment"]
    }
    
    return vente

def main():
    """Fonction principale pour générer et envoyer des ventes"""
    print("🚀 Démarrage du producteur de ventes...")
    
    producer = create_producer()
    vente_id = 1
    
    print(f"📤 Envoi des transactions vers le topic '{TOPIC_NAME}'")
    print("=" * 80)
    
    try:
        while True:
            vente = generate_vente(vente_id)
            
            # Envoi à Kafka
            producer.send(TOPIC_NAME, value=vente)
            producer.flush()
            
            # Affichage
            print(f"✓ Vente #{vente['vente_id']:04d} | "
                  f"Client: {vente['client_nom']:20s} | "
                  f"Produit: {vente['produit_nom']:20s} | "
                  f"Montant: {vente['montant']:8.2f}€ | "
                  f"Pays: {vente['pays']}")
            
            vente_id += 1
            time.sleep(random.uniform(1, 3))  # Délai aléatoire entre 1 et 3 secondes
            
    except KeyboardInterrupt:
        print("\n⏹️  Arrêt du producteur...")
    finally:
        producer.close()
        print("✅ Producteur fermé proprement")

if __name__ == "__main__":
    main()