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


Vérifier que le conteneur Airflow peut communiquer avec le serveur OpenMetadata via le réseau Docker :
docker exec -it airflow-webserver \
curl http://openmetadata:8585/api/v1/system/version
```