# Redpanda-streaming-tickets
Ce projet met en place une pipeline de traitement de tickets clients en temps réel, basée sur un écosystème **Kafka (Redpanda)**, **Apache Spark Structured Streaming** et **AWS S3**. L’architecture est conteneurisée via **Docker Compose**.

## Prérequis

Avant de démarrer, assure-toi d’avoir :

- Docker & Docker Compose installés sur ta machine
- Un compte AWS valide
- Un bucket S3 existant
- Une paire de clés d'accès AWS (Access Key ID + Secret Access Key)
- [Facultatif] Un environnement Python local si tu veux tester certains scripts sans Docker

---

## Architecture

Ce diagramme illustre le flux de données complet de la pipeline ETL, depuis la génération des tickets côté producteur jusqu'à leur analyse finale dans un notebook, en passant par Kafka (Redpanda), le traitement PySpark et le stockage dans AWS S3.


```mermaid
flowchart LR
    create_ticket[create_ticket.py]
    kafka[(Kafka - Redpanda)]
    spark[Spark_traitement.py]
    s3[(AWS S3 - Parquet)]
    notebook[notebook_analysis.ipynb]

    create_ticket --> kafka
    kafka --> spark
    spark --> s3
    s3 --> notebook

    subgraph Producteur
        create_ticket
    end

    subgraph Kafka_Streaming
        kafka
    end

    subgraph Traitement_PySpark
        spark
    end

    subgraph Stockage_Cloud
        s3
    end

    subgraph Analyse
        notebook
    end
```



## Composants
- **Redpanda (Kafka-compatible)** : cluster 3 nœuds pour le streaming

- **Apache Spark** : cluster 1 master / 1 worker pour le traitement des flux

- **Producteur Python** : génère des tickets aléatoires et les publie dans Kafka

- **Consumer PySpark** : agrège les données et les écrit dans AWS S3 (format Parquet)

- **Jupyter Notebook** : lit les données depuis S3 pour les analyser

- **Docker Compose** : orchestration complète de l'infrastructure

## Lancement du projet
1. Cloner le repo
   ```bash
   git clone https://github.com/zaracky/Redpanda-streaming-tickets.git
   
 2. Configurer les variables d'environnement

Modifier le fichier .env à la racine du projet avec vos informations

3. Lancer tous les services
   ```bash
   docker-compose up --build

Cela va :

Lancer Redpanda, Spark Master & Worker

Démarrer le producteur Kafka (génération de tickets)

Lancer le consumer PySpark (streaming + stockage)

Démarrer Jupyter Notebook pour visualiser les résultats

## Accès et Surveillance
Voici les interfaces disponibles une fois les services lancés :

- Console Redpanda : http://localhost:8080

- Interface Spark Master (UI) : http://localhost:8081

- Jupyter Notebook : http://localhost:8888


## Structure du Projet

      ├── Docker/
         ├── Dockerfile.generator        
         ├── Dockerfile.notebook        
         ├── Dockerfile.pyspark          
         ├── docker-compose.yml           
         └── requirements_pyspark.txt
         └── requirements_pyspark.txt
         └── requirements.txt
      ├── .env
      ├── Spark_traitement.py
      ├── create_ticket.py
      ├── notebook_analysis.ipynb
      ├── README.md

## Description des Composants
- Docker/ : Contient tous les Dockerfiles nécessaires à chaque composant de la pipeline ainsi que le fichier docker-compose.yml pour tout orchestrer.

- .env : Fichier contenant les variables sensibles et de configuration (non versionné !).

- Spark_traitement.py : Script Spark Streaming lisant les flux Kafka, enrichissant et sauvegardant les données dans AWS S3.

- create_ticket.py : Générateur de données simulant des tickets clients.

- notebook_analysis.ipynb : Analyse visuelle et statistique des données de tickets stockées en Parquet dans S3.

- README.md : Documentation complète du projet.

## Démonstration

👉 [Demonstration](https://www.loom.com/share/da945a77aafb46f69410f3d57ecc5763?sid=a8942c7c-8607-4f4a-bf0a-663f9e5a6a67)

