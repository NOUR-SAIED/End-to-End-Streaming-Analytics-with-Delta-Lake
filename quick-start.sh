#!/bin/bash

# Script de démarrage rapide pour le projet Lakehouse
# Couleurs pour l'affichage
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=================================================${NC}"
echo -e "${BLUE}   🚀 Lakehouse Streaming - Quick Start${NC}"
echo -e "${BLUE}=================================================${NC}\n"

# Fonction pour afficher les étapes
step() {
    echo -e "\n${GREEN}➜ $1${NC}"
}

error() {
    echo -e "\n${RED}✗ Erreur: $1${NC}"
    exit 1
}

warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Vérifier que Docker est installé
step "Vérification de Docker..."
if ! command -v docker &> /dev/null; then
    error "Docker n'est pas installé. Veuillez l'installer d'abord."
fi
echo "✓ Docker trouvé: $(docker --version)"

# Vérifier que Docker Compose est installé
step "Vérification de Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    error "Docker Compose n'est pas installé. Veuillez l'installer d'abord."
fi
echo "✓ Docker Compose trouvé: $(docker-compose --version)"

# Créer la structure de répertoires
step "Création de la structure de répertoires..."
mkdir -p data/delta/bronze data/delta/silver data/delta/checkpoints scripts
echo "✓ Répertoires créés"

# Vérifier les fichiers requis
step "Vérification des fichiers requis..."
required_files=("docker-compose.yml" "Dockerfile.spark-client" "producer_ventes.py" "spark_streaming_delta.py" "dashboard_analysis.py")
missing_files=()

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        missing_files+=("$file")
    fi
done

if [ ${#missing_files[@]} -gt 0 ]; then
    error "Fichiers manquants: ${missing_files[*]}"
fi
echo "✓ Tous les fichiers requis sont présents"

# Nettoyer les anciens conteneurs si présents
step "Nettoyage des anciens conteneurs..."
docker-compose down -v 2>/dev/null
echo "✓ Nettoyage effectué"

# Démarrer l'infrastructure
step "Démarrage de l'infrastructure (cela peut prendre 2-3 minutes)..."
docker-compose up -d --build

if [ $? -ne 0 ]; then
    error "Échec du démarrage de Docker Compose"
fi

# Attendre que les services soient prêts
step "Attente du démarrage des services..."
echo "⏳ Zookeeper..."
sleep 10
echo "⏳ Kafka..."
sleep 15
echo "⏳ Spark..."
sleep 10

# Vérifier l'état des services
step "Vérification de l'état des services..."
docker-compose ps

# Attendre que Kafka soit complètement prêt
step "Vérification de Kafka..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker exec lh_kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        echo "✓ Kafka est prêt"
        break
    fi
    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        error "Kafka n'a pas démarré dans les temps"
    fi
    echo "⏳ Attente de Kafka... ($attempt/$max_attempts)"
    sleep 2
done

echo -e "\n${GREEN}=================================================${NC}"
echo -e "${GREEN}   ✅ Infrastructure démarrée avec succès !${NC}"
echo -e "${GREEN}=================================================${NC}\n"

# Instructions pour l'utilisateur
echo -e "${BLUE}📋 Prochaines étapes :${NC}\n"

echo -e "${YELLOW}Terminal 1 - Producteur de ventes :${NC}"
echo "docker exec -it lh_spark_client python /app/producer_ventes.py"
echo ""

echo -e "${YELLOW}Terminal 2 - Streaming Spark (après le producteur) :${NC}"
echo "docker exec -it lh_spark_client spark-submit \\"
echo "  --master spark://spark-master:7077 \\"
echo "  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-spark_2.12:2.2.0 \\"
echo "  /app/spark_streaming_delta.py"
echo ""

echo -e "${YELLOW}Terminal 3 - Dashboard (après 2-3 minutes) :${NC}"
echo "docker exec -it lh_spark_client spark-submit \\"
echo "  --master spark://spark-master:7077 \\"
echo "  --packages io.delta:delta-spark_2.12:2.2.0 \\"
echo "  /app/dashboard_analysis.py"
echo ""

echo -e "${BLUE}🌐 Interface Web Spark :${NC}"
echo "http://localhost:8080"
echo ""

echo -e "${BLUE}📚 Pour plus d'informations, consultez le guide complet.${NC}"
echo -e "${GREEN}=================================================${NC}\n"

# Option pour démarrer automatiquement le producteur
read -p "Voulez-vous démarrer automatiquement le producteur ? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "\n${GREEN}Démarrage du producteur...${NC}"
    docker exec -d lh_spark_client python /app/producer_ventes.py
    echo "✓ Producteur démarré en arrière-plan"
    echo ""
    echo -e "${YELLOW}Pour voir les logs du producteur :${NC}"
    echo "docker logs -f lh_spark_client"
fi

echo -e "\n${GREEN}✨ Tout est prêt ! Bon streaming !${NC}\n"