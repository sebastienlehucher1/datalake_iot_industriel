# Brief : créer et maintenir un DataLake - IoT Industriel


## Présentation :

Je suis Data Engineer chez IndustrIA, une ESN spécialisée dans la valorisation des données industrielles. Mon client, un équipementier automobile, exploite 5 lignes de production instrumentées de capteurs (température, pression, temps de fonctionnement). Les données sont aujourd'hui stockées en vrac, sans structure ni gouvernance. La DSI me confie la mission de concevoir et déployer un data-lake moderne pour centraliser, documenter et sécuriser l'ensemble de ces flux, en vue d'un futur projet de maintenance prédictive.

Les données synthétiques proviennnent de relevés de capteurs de différentes lignes de production industrielle. Ces données représentent des mesures de température, de pression et, dans certains cas, de temps écoulé de machines industrielles, avec des enregistrements des conditions de fonctionnement normales et des anomalies potentielles.



## Technologies du projet :

- Conteneurisation Docker :
    - PostgreSQL : Metastore (bases de données interne d'Airflow et du catalogue OpenMetadata)
    - MinIo
    - MinIo-init    
    - Opensearch
    - Execute-migrate-all
    - Openmetadata
    - Openmetadata-ingestion
    - Airflow-init
    - Airflow-webserver
    - Airflow-scheduler
    

- MinIo : serveur de stockage d'objets open source, compatible avec l'API Amazon S3

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

La liste des commandes docker utilisées dans ce projet se trouve dans le fichier "commandes_docker.md".

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




