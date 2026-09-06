## Commandes Docker :
```
Chercher les paquets Python qui sont installés dans le conteneur airflow-webserver dont le nom contient "openmetadata" :
docker exec -it airflow-webserver python -m pip list | grep openmetadata
docker exec -it airflow-webserver bash -lc "pip list | grep -i openmetadata"


Vérifier que le package "openmetadata-ingestion" est bien installé dans l'environnement Python d'Airflow :
docker exec -it airflow-webserver bash -lc "python -m pip show openmetadata-ingestion"


Vérifier que le package "openmetadata-managed-apis" est bien installé dans l'environnement Python d'Airflow :
docker exec -it airflow-webserver bash -lc "python -m pip show openmetadata-managed-apis"


Vérifier que le plugin OpenMetadata est chargé dans Airflow :
docker exec -it airflow-webserver airflow plugins


Vérifier que le conteneur airflow-webserver peut communiquer avec le serveur OpenMetadata via le réseau Docker :
docker exec -it airflow-webserver \
curl http://openmetadata:8585/api/v1/system/version


Installer curl en root sur le conteneur openmetadata-server :
docker exec -u 0 -it openmetadata-server apk add --no-cache curl


Vérifier que le conteneur openmetadata-server communique bien avec Airflow via le réseau Docker :
docker exec -it openmetadata-server \
  curl -i http://airflow-webserver:8080/api/v1/health


Tester l'authentification Airflow avec les identifiants configurés depuis le conteneur openmetadata-server :
docker exec -it openmetadata-server \
  curl -i -u admin:pass-admin http://airflow-webserver:8080/api/v1/users


Créer l'extension pg_stat_statements dans la base de données utilisée par OpenMetadata :
docker exec -it psql-db psql \
  -U openmetadata \
  -d openmetadata_db \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"


Vérifier la création de pg_stat_statements dans la base Openmetadata :
docker exec -it psql-db psql \
  -U openmetadata \
  -d openmetadata_db \
  -c \
"SELECT extname, extversion
 FROM pg_extension
 WHERE extname = 'pg_stat_statements';"
```

## Commandes psql (dans le conteneur psql-db) :
```
Vérifier que l'extension PostgreSQL pg_stat_statements soit disponible dans la base :
psql -U postgres -d postgres_db -c \
"SELECT name, default_version, installed_version
 FROM pg_available_extensions
 WHERE name = 'pg_stat_statements';"


Ajouter pg_stat_statements à shared_preload_libraries :
 psql -U postgres -d postgres_db -c \
"ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';"


Vérifier l'ajout de pg_stat_statements à shared_preload_libraries :
cat $(psql -U postgres -d postgres_db -tAc "SHOW config_file" | xargs dirname)/postgresql.auto.conf



```