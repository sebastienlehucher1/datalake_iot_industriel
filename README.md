# Brief : créer et maintenir un DataLake - IoT Industriel


## Présentation :

Je suis Data Engineer chez IndustrIA, une ESN spécialisée dans la valorisation des données industrielles. Mon client, un équipementier automobile, exploite 5 lignes de production instrumentées de capteurs (température, pression, temps de fonctionnement). Les données sont aujourd'hui stockées en vrac, sans structure ni gouvernance. La DSI me confie la mission de concevoir et déployer un Data Lake moderne pour centraliser, documenter et sécuriser l'ensemble de ces flux, en vue d'un futur projet de maintenance prédictive.

Les données synthétiques proviennnent de relevés de capteurs de différentes lignes de production industrielle. Ces données représentent des mesures de température, de pression et, dans certains cas, de temps écoulé de machines industrielles, avec des enregistrements des conditions de fonctionnement normales et des anomalies potentielles.

Dans une perspective de **maintenance prédictive**, disposer de données fiables, documentées et harmonisées est la condition *sine qua non* du succès. Les modèles d'intelligence artificielle dédiés à la détection d'anomalies reposent sur des historiques massifs de télémétrie multi-équipements. 

Ce projet met en place l'architecture **Data Lake (MinIO, Airflow, OpenMetadata)** qui sert de fondation technique pour qualifier ces données. À terme, cette infrastructure permettra d'entraîner des modèles capables d'anticiper les signes précurseurs de défaillance et de réduire drastiquement les arrêts non planifiés des lignes de production.



## Technologies du projet :

- Conteneurisation Docker :
    - PostgreSQL : Metastore (bases de données interne d'Airflow et du catalogue OpenMetadata)
    - MinIo
    - MinIo-init    
    - Opensearch
    - Execute-migrate-all
    - Openmetadata-server
    - Openmetadata-ingestion
    - Airflow-init
    - Airflow-webserver
    - Airflow-scheduler
    

- MinIo : serveur de stockage d'objets open source, compatible avec l’API S3 d’Amazon Web Services (AWS)

- boto3 : bibliothèque Python officielle d’AWS (AWS SDK for Python), qui permet d’utiliser S3 AWS

- Apache Airflow : permet l'automatisation et le monitoring des pipelines

- OpenMetadata : plateforme open source de catalogue et de gouvernance des données, permettant de documenter les métadonnées et de suivre le lineage des données



## Structure des données initiales :

Format commun :
Tous les jeux de données sont au format CSV et comportent les champs communs suivants :
- timestamp : Date et heure de la mesure (format AAAA-MM-JJ HH:MM:SS)
- temperature/Temperature : Valeur de la température en unités arbitraires
- pressure/Pressure : Valeur de la pression en unités arbitraires
- label : Indicateur binaire (0 = fonctionnement normal, 1 = anomalie)

Certains ensembles de données incluent un champ supplémentaire :
- elapsed_time/Elapsed_time : Temps d'exécution de la machine en unités arbitraires



## Modélisation de l'architecture en couches Raw / Staging / Curated / Archive :

Pour des données provenant de 5 fichiers CSV avec des différences de schéma entre lignes, un pipeline ETL serait :


1. Raw layer (données brutes)

    Objectif : conserver une trace intégrale pour l’audit.
    
    - On stocke exactement ce qui est reçu, sans transformation.
    - Chaque fichier garde ses noms de colonnes initiaux (Temperature, temperature, etc.).
    

2. Staging layer (pré-traitement)

    On commence à uniformiser les schémas :
    - Renommer les colonnes pour standardiser (temperature, Pressure, elapsed_time)
    - Ajuster les types de données
    - Ajouter des colonnes manquantes avec NULL si nécessaire
    - Validation des valeurs


3. Curated layer (données prêtes à l’usage)

    - Schéma final, stable et homogène
    - Données nettoyées, enrichies et prêtes pour l’analytique ou le ML
    - Toutes les colonnes sont présentes et correctement typées


4. Archive layer

    - Conservation des anciennes versions ou historiques
    - Peut contenir soit les fichiers Raw, soit les snapshots du Curated
    - Sert à l’audit ou à la reconstruction si nécessaire



## Lancement du projet :

La première étape consiste à cloner le dépôt Git depuis l'URL en ligne vers votre machine locale. Cela crée une copie complète du projet, y compris tout l'historique des commits.
Ouvrez votre terminal de VS Code ou votre invite de commandes et utilisez la commande "git clone https://github.com/sebastienlehucher1/datalake_iot_industriel.git". Cette commande va créer un dossier avec le nom du dépôt "datalake_iot_industriel" et télécharger tous les fichiers du projet à l'intérieur.

Après avoir cloné le dépôt, vous devez vous déplacer dans le dossier qui vient d'être créé pour pouvoir travailler sur le projet en utilisant la commande "cd datalake_iot_industriel" dans votre terminal de VS Code.

La liste des commandes docker + psql utilisées dans ce projet se trouve dans le fichier "commandes_docker_psql.md".

### Construction des conteneurs Docker :
```
docker compose build
docker compose up -d
```

### Accès à l'interface d'Airflow dans le navigateur de l'utilisateur

- Le projet est à présent lancé, l'interface d'Airflow est disponible à l'adresse suivante: \
http://localhost:8080/home

- Visualisation des DAGs (Directed Acyclic Graph) : pipelines composés de tasks


### Accès à l'interface de MinIo dans le navigateur de l'utilisateur

- L'interface de MinIo est disponible à l'adresse suivante: \
http://localhost:9001/home


### Accès à l'interface de OpenMetadata dans le navigateur de l'utilisateur

- L'interface de OpenMetadata est disponible à l'adresse suivante: \
http://localhost:8585/home (admin@open-metadata.org / admin par défaut pour se connecter)



## Configuration de l'UI OpenMetadata (v1.8.2.0)

Cette section récapitule la gouvernance, les rôles et les interconnexions de services configurés directement depuis l'interface d'administration OpenMetadata.

---

### 1. Classification & Taggage (Data Governance)

* **Classification :** `DataClassification`
  * **Description :** *Classification des variables métiers et techniques du Data Lake, utilisée pour structurer la gouvernance, identifier les cibles de modélisation IA/ML et appliquer des règles de qualité de données.*
* **Tag :** `TargetVariable`
  * **Rattaché à :** `DataClassification.TargetVariable`
  * **Description :** *Colonne cible (variable dépendante / ground truth) destinée aux modèles d'apprentissage automatique (ML/IA) pour la détection d'anomalies et la prédiction de pannes. Ne doit pas être utilisée comme variable explicative (pour éviter la fuite de données) et exige une qualité stricte sans aucune valeur nulle.*

---

### 2. Gestion des Utilisateurs & Rôles

* **Utilisateur :** `responsable_maintenance`
  * **Rôle/Périmètre :** Utilisateur dédié à la gestion opérationnelle, au suivi des pipelines d'ingestion et à la maintenance de la qualité des métadonnées sur le catalogue.

---

### 3. Interconnexions des Services

#### A. Service Base de Données — `Postgres_Production`
* **Type :** Service PostgreSQL (Database Service)
* **Usage :** Ingestion du schéma et des métadonnées du catalogue applicatif/métier.
* **Paramètres de connexion :**
  * **Host & Port :** `psql-db:5432`
  * **Database Name :** `${OM_DB}` 
  * **Database Username :** `${OM_DB_USER}`
  * **Prérequis d'ingestion (GetQueries & Profiler) :** Extension `pg_stat_statements` active et attribution du rôle `pg_read_all_stats` à l'utilisateur `openmetadata_user`.

#### B. Service Pipeline — `Airflow`
* **Type :** Service Pipeline (Pipeline Service)
* **Usage :** Orchestration et suivi des DAGs d'ingestion internes d'OpenMetadata.
* **Paramètres de connexion :**
  * **Endpoint URL :** `http://openmetadata-ingestion:8080` *(Utilisation du nom du service Docker `openmetadata-ingestion` pour la résolution DNS interne au réseau conteneurisé, en remplacement de `localhost`)*.
  * **Host & Port :** `psql-db:5432`
  * **Database Name :** `${OM_AIRFLOW_DB}`
  * **Database Username :** `${OM_AIRFLOW_DB_USER}`
  * **Note réseau :** Connecté au schéma interne isolé d'Airflow (distinct de la base Airflow métier d'entreprise).

#### C. Service Stockage Objets — `MinIO_Raw`
* **Type :** Service Storage (S3 / MinIO Compatible)
* **Usage :** Découverte et suivi de lignée sur les buckets de données brutes (`raw`).
* **Paramètres de connexion :**
  * **Endpoint URL :** `http://minio:9000` *(Utilisation du nom du service Docker `minio` pour la résolution DNS interne au réseau conteneurisé, en remplacement de `localhost`)*.
  * **Bucket cible :** `raw`




