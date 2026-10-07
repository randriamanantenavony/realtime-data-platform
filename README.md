# Real-Time Tech Sentiment Analytics Platform

Plateforme de Data Engineering temps réel permettant de collecter des publications depuis Bluesky, de les transporter avec Apache Kafka, de les traiter avec Apache Spark, de les stocker dans Delta Lake selon une architecture Medallion, puis d'analyser automatiquement leur sentiment grâce à un modèle NLP multilingue.

Les données enrichies sont ensuite transformées en indicateurs métier dans une couche Gold et visualisées dans un dashboard Streamlit actualisé automatiquement.

---

## Objectif

L'objectif du projet est de construire une plateforme complète capable de :

- collecter des publications Bluesky en temps réel ;
- publier les événements dans Apache Kafka ;
- consommer les événements avec Spark Structured Streaming ;
- stocker les données avec Delta Lake ;
- organiser les traitements selon une architecture Bronze, Silver et Gold ;
- nettoyer et filtrer les publications liées aux technologies ;
- analyser automatiquement leur contenu textuel ;
- classifier les publications en `positive`, `neutral` ou `negative` ;
- conserver un score de confiance pour chaque prédiction ;
- produire des indicateurs métier avec PySpark et Spark SQL ;
- suivre les technologies les plus discutées ;
- visualiser les résultats dans un dashboard Streamlit.

Ce projet a été réalisé comme projet portfolio orienté Data Engineering, Streaming, Big Data et NLP.

---

## Architecture

Le pipeline suit l'architecture suivante :

**Bluesky Jetstream → Python → Apache Kafka → Spark Structured Streaming → Bronze Delta → Silver Tech → NLP Sentiment → Silver Enriched → Gold → Streamlit**

Les différentes responsabilités sont séparées entre ingestion, stockage, transformation, enrichissement NLP, préparation analytique et visualisation.

---

## Technologies utilisées

### Data Engineering

- Python
- Apache Kafka
- Apache Spark
- PySpark
- Spark Structured Streaming
- Spark SQL
- Delta Lake
- Docker
- Docker Compose

### Machine Learning et NLP

- Hugging Face Transformers
- XLM-RoBERTa
- PyTorch
- Pandas UDF
- Apache Arrow

### Visualisation

- Streamlit
- Pandas

### Concepts appliqués

- Event-Driven Architecture
- Streaming Data Pipeline
- Medallion Architecture
- Bronze / Silver / Gold
- Delta Lake
- Micro-batch processing
- Checkpointing
- Distributed Computing
- NLP inference

---

# Pipeline de données

## 1. Ingestion Bluesky

Le fichier `src/ingestion/bluesky_source.py` contient la logique permettant de se connecter à Bluesky Jetstream à travers une connexion WebSocket.

Les événements sont filtrés afin de conserver uniquement :

- les commits ;
- les opérations de création ;
- les publications `app.bsky.feed.post` ;
- les publications contenant du texte.

Les événements sont ensuite normalisés avec notamment :

- `event_id`
- `source`
- `author_id`
- `timestamp`
- `text`
- `language`
- `url`
- `ingested_at`

Le fichier `src/producers/kafka_producer.py` prend en charge la publication des événements normalisés dans Kafka.

Le topic Kafka utilisé est :

`social-events`

---

## 2. Kafka vers Bronze

Le fichier `src/streaming/kafka_stream_reader.py` utilise Spark Structured Streaming pour consommer continuellement les événements Kafka.

Les données reçues depuis Kafka sont :

- converties en chaînes de caractères ;
- parsées depuis le JSON ;
- validées avec un schéma Spark ;
- enrichies avec les métadonnées Kafka.

Les métadonnées conservées comprennent notamment :

- `topic`
- `partition`
- `offset`
- `kafka_timestamp`

Les événements sont ensuite enregistrés au format Delta dans :

`data/bronze/social_events`

---

# Couche Bronze

La couche Bronze constitue la couche d'ingestion du Data Lake.

Elle permet de conserver les événements proches de leur état d'origine avant les transformations métier.

Elle permet notamment :

- de préserver les données sources ;
- de conserver les métadonnées Kafka ;
- de séparer ingestion et transformation ;
- de rejouer les traitements ;
- de bénéficier du versioning et du journal transactionnel Delta Lake.

Les données Bronze sont stockées dans :

`data/bronze/social_events`

---

# Couche Silver

## Silver Tech

Le fichier `src/streaming/bronze_to_silver.py` lit les données Bronze avec Spark Structured Streaming.

Le pipeline applique plusieurs transformations.

### Nettoyage

Les publications dont le texte est `NULL` ou vide sont supprimées.

### Filtrage technologique

Seules les publications relatives à des sujets technologiques sont conservées.

Les principaux mots-clés couvrent notamment les domaines suivants.

Intelligence artificielle :

- Artificial Intelligence
- ChatGPT
- OpenAI
- Copilot
- Gemini
- Claude
- Grok
- LLM
- Machine Learning
- Deep Learning
- Generative AI
- GenAI

Data :

- Data Science
- Data Engineering
- Big Data
- Spark
- PySpark
- Kafka
- Databricks
- Delta Lake

Cloud :

- Azure
- AWS
- Google Cloud

Programmation :

- Python
- Java
- JavaScript
- TypeScript

Entreprises et plateformes :

- Microsoft
- NVIDIA
- GitHub

### Déduplication

Les événements sont dédupliqués à partir de leur `event_id`.

Les publications retenues sont stockées dans :

`data/silver/tech_events`

---

# Analyse de sentiment

Le fichier `src/pipelines/silver_sentiment.py` enrichit les publications technologiques avec une analyse NLP.

Le modèle utilisé est :

`cardiffnlp/twitter-xlm-roberta-base-sentiment`

Le modèle classe les publications selon trois catégories :

- `positive`
- `neutral`
- `negative`

Chaque prédiction contient également un score de confiance.

Exemple :

Publication :

`Python is absolutely amazing!`

Résultat :

- `sentiment = positive`
- `sentiment_score = 0.9299`

Les données enrichies contiennent notamment :

- `event_id`
- `source`
- `author_id`
- `timestamp`
- `text`
- `language`
- `sentiment`
- `sentiment_score`

Elles sont enregistrées dans :

`data/silver/sentiment_events`

---

## Pandas UDF et traitement distribué

L'intégration du modèle NLP avec Spark est réalisée avec une Pandas UDF.

Spark distribue les données en partitions. Les workers Python reçoivent ensuite des batches de textes grâce à Pandas et Apache Arrow.

Le modèle XLM-RoBERTa est initialisé au niveau du worker puis réutilisé pour traiter plusieurs batches.

Cette architecture évite de ramener l'ensemble des données sur le driver et conserve la logique distribuée de Spark.

---

# Couche Gold

La couche Gold transforme les publications enrichies en données analytiques orientées métier.

Les agrégations utilisent PySpark et Spark SQL.

Les tables Gold servent directement de source au dashboard Streamlit.

---

## Sentiment Summary

Les données sont stockées dans :

`data/gold/sentiment_summary`

Cette table contient la distribution globale des sentiments.

Exemple :

| Sentiment | Nombre de publications |
|---|---:|
| Neutral | 128 |
| Negative | 82 |
| Positive | 38 |

Cette table permet de répondre à la question :

**Quel est le sentiment global des publications technologiques analysées ?**

---

## Sentiment By Language

Les données sont stockées dans :

`data/gold/sentiment_by_language`

Cette table contient notamment :

- `language`
- `sentiment`
- `post_count`

Elle permet d'analyser la répartition des sentiments selon les différentes langues détectées dans les publications.

---

## Sentiment Over Time

Les données sont stockées dans :

`data/gold/sentiment_over_time`

Cette table contient :

- `event_date`
- `sentiment`
- `post_count`

Elle permet de suivre l'évolution des sentiments dans le temps et alimente le graphique temporel du dashboard.

---

## Technology Trends

Les données sont stockées dans :

`data/gold/technology_trends`

Cette table analyse les technologies détectées dans les publications et leur sentiment associé.

Elle contient notamment :

- `technology`
- `sentiment`
- `post_count`

Elle permet notamment de répondre à la question :

**Quelles technologies sont les plus discutées et quel sentiment leur est associé ?**

---

# Gold Streaming

Le fichier `src/pipelines/gold_streaming.py` automatise l'actualisation de la couche Gold.

Il écoute les nouvelles données disponibles dans :

`data/silver/sentiment_events`

Lorsqu'un nouveau micro-batch est détecté, celui-ci agit comme déclencheur.

Le pipeline recalcule ensuite les principaux indicateurs analytiques à partir de l'état courant des données Silver enrichies.

Les tables suivantes sont actualisées :

- `sentiment_summary`
- `sentiment_by_language`
- `sentiment_over_time`
- `technology_trends`

La couche Gold représente ainsi le snapshot analytique courant consommé par le dashboard.

Cette approche convient au volume actuel du projet et privilégie la simplicité d'implémentation.

Une approche plus scalable pourrait utiliser des mises à jour incrémentales avec Delta `MERGE`.

---

# Dashboard Streamlit

Le dashboard est implémenté dans :

`src/dashboard/app.py`

Il lit les différentes tables Gold et affiche les principaux indicateurs du projet.

## Indicateurs globaux

Le dashboard présente :

- nombre total de publications ;
- nombre et pourcentage de publications positives ;
- nombre et pourcentage de publications neutres ;
- nombre et pourcentage de publications négatives.

## Distribution globale du sentiment

Une visualisation présente la répartition entre :

- Positive
- Neutral
- Negative

## Sentiment Over Time

Un graphique permet de suivre l'évolution des sentiments dans le temps.

## Sentiment By Language

Une visualisation compare les sentiments entre les différentes langues présentes dans les publications.

## Technology Trends

Une visualisation permet de comparer les technologies les plus discutées et leur sentiment associé.

## Actualisation

Le dashboard vérifie régulièrement les dernières données disponibles dans Gold et met à jour les visualisations lorsque les tables évoluent.

---

# Organisation des données

```text
data/
├── bronze/
│   └── social_events/
│
├── silver/
│   ├── tech_events/
│   └── sentiment_events/
│
└── gold/
    ├── sentiment_summary/
    ├── sentiment_by_language/
    ├── sentiment_over_time/
    └── technology_trends/
```

---

# Checkpoints

Chaque pipeline Structured Streaming possède son propre répertoire de checkpoint.

```text
checkpoints/
├── bronze/
├── silver/
├── sentiment/
└── gold/
```

Les checkpoints permettent à Spark de mémoriser la progression de chaque requête streaming.

Ils conservent notamment :

- les offsets déjà traités ;
- les micro-batches validés ;
- les métadonnées de la requête ;
- certains états nécessaires à la reprise.

Les checkpoints ne représentent pas les données métier.

---

# Delta Lake

Delta Lake constitue la couche de stockage principale du projet.

Chaque table Delta possède un répertoire `_delta_log` contenant son historique transactionnel.

Delta Lake apporte notamment :

- transactions ACID ;
- gestion et contrôle du schéma ;
- versioning ;
- Time Travel ;
- intégration avec Spark Structured Streaming ;
- reprise fiable après interruption.

---

# Delta Lake Time Travel

L'historique d'une table Delta peut être consulté avec PySpark :

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(
    spark,
    silver_path
)

delta_table.history().show(
    truncate=False
)
```

Une version précise peut ensuite être relue :

```python
df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 10)
    .load(silver_path)
)
```

Cela permet notamment d'auditer les différentes versions d'une table ou d'examiner un état antérieur des données.

---

# Structure du projet

```text
realtime-data-platform/
│
├── config/
│   └── config.yml
│
├── data/
│   ├── bronze/
│   │   └── social_events/
│   │
│   ├── silver/
│   │   ├── tech_events/
│   │   └── sentiment_events/
│   │
│   └── gold/
│       ├── sentiment_summary/
│       ├── sentiment_by_language/
│       ├── sentiment_over_time/
│       └── technology_trends/
│
├── checkpoints/
│   ├── bronze/
│   ├── silver/
│   ├── sentiment/
│   └── gold/
│
├── src/
│   ├── ingestion/
│   │   ├── bluesky_source.py
│   │   └── bluesky_producer.py
│   │
│   ├── producers/
│   │   └── kafka_producer.py
│   │
│   ├── processing/
│   │   └── sentiment.py
│   │
│   ├── streaming/
│   │   ├── kafka_stream_reader.py
│   │   └── bronze_to_silver.py
│   │
│   ├── pipelines/
│   │   ├── silver_sentiment.py
│   │   ├── gold.py
│   │   └── gold_streaming.py
│   │
│   └── dashboard/
│       └── app.py
│
├── tests/
│   ├── test_sentiment.py
│   ├── test_sentiment_silver.py
│   └── test_sentiment_spark.py
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Prérequis

Le projet nécessite notamment :

- Python
- Java
- Apache Spark
- Docker
- Docker Compose

Le projet a été développé localement sous Windows avec Apache Spark 4.2.0 et Scala 2.13.

---

# Installation des dépendances

Il est recommandé d'utiliser un environnement virtuel Python.

Installer ensuite les dépendances du projet :

```bash
pip install -r requirements.txt
```

Les principales dépendances utilisées sont notamment :

- PySpark
- PyYAML
- WebSockets
- client Kafka Python
- Pandas
- PyArrow
- PyTorch
- Transformers
- SentencePiece
- Streamlit
- Delta Lake Python client

---

# Lancement du pipeline complet

Les composants du pipeline doivent être lancés dans plusieurs terminaux et rester actifs simultanément.

Toutes les commandes suivantes sont à exécuter depuis la racine du projet.

## 1. Démarrer Kafka avec Docker

```bash
docker compose up -d
```

Vérifier que les conteneurs sont actifs :

```bash
docker compose ps
```

Vérifier que le broker Kafka répond :

```bash
docker compose exec broker /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

---

## 2. Démarrer l'ingestion Bluesky

Dans un nouveau terminal :

```bash
python src/ingestion/bluesky_producer.py
```

Ce processus se connecte à Bluesky Jetstream et transmet les publications normalisées à Kafka.

Le fichier `bluesky_source.py` contient la logique liée à la source Bluesky tandis que `kafka_producer.py` gère la publication dans Kafka.

Laisser ce processus actif.

---

## 3. Démarrer Kafka vers Bronze

Dans un nouveau terminal :

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_4.2_2.13:4.4.0 src\streaming\kafka_stream_reader.py
```

Ce processus consomme les événements Kafka avec Spark Structured Streaming et les écrit dans :

`data/bronze/social_events`

Laisser ce processus actif.

---

## 4. Démarrer Bronze vers Silver Tech

Dans un nouveau terminal :

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_4.2_2.13:4.4.0 src\streaming\bronze_to_silver.py
```

Ce pipeline :

- lit Bronze ;
- supprime les textes invalides ;
- filtre les publications technologiques ;
- déduplique les événements ;
- écrit les résultats dans Silver.

Les données sont enregistrées dans :

`data/silver/tech_events`

Laisser ce processus actif.

---

## 5. Démarrer l'analyse de sentiment

Dans un nouveau terminal :

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_4.2_2.13:4.4.0 src\pipelines\silver_sentiment.py
```

Ce pipeline :

- lit `tech_events` ;
- applique la Pandas UDF ;
- exécute XLM-RoBERTa ;
- calcule `sentiment` ;
- calcule `sentiment_score` ;
- écrit les publications enrichies.

Les résultats sont enregistrés dans :

`data/silver/sentiment_events`

Laisser ce processus actif.

---

## 6. Démarrer Gold Streaming

Dans un nouveau terminal :

```bash
spark-submit --packages io.delta:delta-spark_2.13:4.2.0 src/pipelines/gold_streaming.py
```

Ce pipeline écoute les nouvelles données disponibles dans Silver Sentiment et actualise automatiquement :

- `sentiment_summary`
- `sentiment_by_language`
- `sentiment_over_time`
- `technology_trends`

Les résultats sont enregistrés dans :

`data/gold/`

Lorsqu'un nouveau micro-batch est traité, le terminal indique l'actualisation de Gold.

Laisser ce processus actif.

---

## 7. Démarrer le dashboard Streamlit

Dans un nouveau terminal :

```bash
streamlit run src/dashboard/app.py
```

Ouvrir ensuite :

`http://localhost:8501`

Le dashboard lit les tables Gold et actualise régulièrement les indicateurs affichés.

---

# Ordre de démarrage résumé

Les composants doivent être démarrés dans cet ordre :

1. Docker et Kafka
2. Bluesky Producer
3. Kafka vers Bronze
4. Bronze vers Silver Tech
5. Silver Tech vers Silver Sentiment
6. Gold Streaming
7. Streamlit

Chaque processus streaming doit rester actif pendant le test de bout en bout.

---

# Vérification du pipeline de bout en bout

Pour vérifier que la plateforme fonctionne correctement :

1. vérifier que `bluesky_producer.py` reçoit des publications ;
2. vérifier que Kafka est actif ;
3. vérifier que `data/bronze/social_events` reçoit de nouvelles données ;
4. vérifier que les publications technologiques arrivent dans `data/silver/tech_events` ;
5. vérifier que `data/silver/sentiment_events` contient `sentiment` et `sentiment_score` ;
6. vérifier que `gold_streaming.py` détecte de nouveaux micro-batches ;
7. vérifier que les tables Gold sont actualisées ;
8. vérifier que le nombre total de publications évolue dans Streamlit.

Le flux complet est :

**Bluesky → Kafka → Bronze → Silver Tech → NLP Sentiment → Gold → Streamlit**

Une publication technologique reçue depuis Bluesky doit ainsi pouvoir traverser l'ensemble du pipeline sans intervention manuelle.

---

# Contraintes de l'environnement local

Le projet est actuellement exécuté sur une machine Windows locale.

Le modèle XLM-RoBERTa est relativement coûteux en mémoire. Lorsque plusieurs workers Python chargent simultanément le modèle, la consommation mémoire peut devenir importante.

Le parallélisme du pipeline NLP peut donc être volontairement limité avec :

```python
.master("local[1]")
```

et :

```python
.coalesce(1)
```

Cette configuration privilégie la stabilité sur une machine locale.

Sur une infrastructure distribuée, Spark pourrait exploiter plusieurs executors et plusieurs machines.

Le recalcul des indicateurs Gold peut également prendre plus de temps que l'intervalle de déclenchement configuré lorsque les ressources de la machine sont limitées.

---

# Limites actuelles

## Détection des technologies

Une publication contenant plusieurs technologies peut actuellement être rattachée principalement à la première technologie reconnue.

Par exemple :

`Spark works really well with Kafka`

peut être associé uniquement à `spark`.

Une évolution future pourra utiliser une structure multi-valeurs et `explode()` afin d'associer une publication simultanément à Spark et Kafka.

## Analyse de sentiment

Le `sentiment_score` correspond au niveau de confiance du modèle pour la classe prédite.

Il ne représente pas directement un pourcentage d'utilisateurs ayant une opinion positive ou négative.

## Gold Streaming

La version actuelle utilise l'arrivée d'un nouveau micro-batch comme déclencheur puis recalcule les indicateurs à partir de l'état courant des données Silver enrichies.

Cette approche est adaptée au volume actuel du projet.

Pour des volumes importants, les indicateurs pourraient être maintenus de manière incrémentale avec `foreachBatch` et Delta `MERGE`.

---

# Améliorations futures

## Déploiement Cloud

Le pipeline pourrait être migré vers :

- Microsoft Azure
- AWS
- Databricks

Le stockage Delta local pourrait être remplacé par :

- Azure Data Lake Storage
- Amazon S3

## Orchestration

Les différents composants pourraient être orchestrés avec :

- Apache Airflow
- Azure Data Factory
- Databricks Workflows

## Optimisation NLP

Plusieurs optimisations sont envisageables :

- modèle NLP plus léger ;
- GPU inference ;
- augmentation contrôlée du batch size ;
- optimisation du nombre de workers ;
- cache explicite du modèle ;
- suivi de la consommation mémoire ;
- mesure du temps d'inférence.

## Monitoring

Une couche d'observabilité pourrait mesurer :

- Kafka consumer lag ;
- nombre d'événements ingérés ;
- durée des micro-batches ;
- temps d'inférence NLP ;
- volumes Bronze, Silver et Gold ;
- erreurs de traitement ;
- latence de bout en bout.

## Data Quality

Des contrôles supplémentaires pourraient être ajoutés :

- validation de schéma ;
- contrôle des valeurs nulles ;
- contrôle des doublons ;
- détection d'anomalies de volume ;
- contrôle des langues ;
- règles de qualité métier.

---

# Compétences démontrées

Ce projet met en pratique des compétences en :

- Data Engineering
- Apache Kafka
- Apache Spark
- PySpark
- Spark Structured Streaming
- Spark SQL
- Delta Lake
- architecture Medallion
- traitement distribué
- Spark DataFrames
- partitions et workers
- Pandas UDF
- Apache Arrow
- NLP
- Transformers
- XLM-RoBERTa
- Docker
- Python
- Streamlit

Le projet couvre l'ensemble du cycle de traitement :

**Source → Ingestion → Streaming → Storage → Transformation → NLP → Analytics → Visualization**

---

# Captures du projet

Les captures du dashboard et de l'architecture peuvent être placées dans :

```text
docs/
└── images/
    ├── architecture.png
    ├── dashboard-overview.png
    ├── sentiment-over-time.png
    └── technology-trends.png
```

Exemple d'intégration dans le README :

```markdown
docs/images/dashboard-overview.png
```

et :

```markdown
docs/images/architecture.png
```

---

# Statut du projet

La version actuelle couvre :

- ingestion Bluesky ;
- production Kafka ;
- consommation Kafka avec Spark ;
- stockage Bronze Delta ;
- transformation Bronze vers Silver ;
- filtrage technologique ;
- déduplication ;
- analyse NLP distribuée ;
- enrichment du sentiment ;
- agrégations avec PySpark et Spark SQL ;
- couche Gold ;
- actualisation Gold Streaming ;
- dashboard Streamlit ;
- actualisation automatique des indicateurs.

---

# Auteur

**Voninjatovo Fanomezantsoa Sarobidy Randriamanantena**

Ingénieure d'étude et de développement

Projet portfolio orienté Data Engineering, Big Data, Streaming et Intelligence Artificielle.