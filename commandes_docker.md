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
```