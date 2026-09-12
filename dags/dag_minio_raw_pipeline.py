from datetime import datetime, timezone
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from dotenv import load_dotenv
import requests
import yaml


#Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration des accès OpenMetadata
OPENMETADATA_HOST = "http://openmetadata-server:8585/api"
JWT_TOKEN = os.getenv("JWT_ACCESS_TOKEN_USER_ADMIN")
YAML_METADATA_DIR = "/opt/airflow/dags/metadata/"
# Identifiant (FQN) de l'ingestion configurée dans l'UI OpenMetadata
PIPELINE_FQN = "MinIO_Raw.MinIO_Raw_Ingestion"

HEADERS = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Content-Type": "application/json",
}


# -------------------------------------------------------------------------
# Fonction d'enrichissement exécutée par Airflow
# -------------------------------------------------------------------------
def run_metadata_enrichment():

    rep_yaml_files_path = Path(YAML_METADATA_DIR)

    if not rep_yaml_files_path.exists():
        raise FileNotFoundError(f"Le dossier '{rep_yaml_files_path}' n'existe pas.")
    
    # Lit tous les fichiers YAML du dossier metadata
    yaml_files = [
        file for file in rep_yaml_files_path.iterdir()
        if file.is_file() and file.suffix == ".yaml"
    ]

    if not yaml_files:
        raise FileNotFoundError(f"Aucun fichier YAML trouvé dans '{rep_yaml_files_path}'.")
        
    print(
        f"Démarrage de l'enrichissement rapide pour {len(yaml_files)}"
        " fiche(s) de métadonnées..."
    )

    # Récupération des conteneurs existants pour trouver le FQN correspondant à la ligne de production
    res = requests.get(
        f"{OPENMETADATA_HOST}/v1/containers?limit=100&fields=dataModel", headers=HEADERS
    )
    if res.status_code != 200:
        raise RuntimeError(f"Erreur API ({res.status_code}) : {res.text}")

    all_containers = res.json().get("data", [])


    # Parcours des fichiers YAML et mise à jour
    for filepath in yaml_files:
   
        with open(filepath, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        container_name = yaml_data.get("name")

        # Extraction sécurisée de la lettre de la ligne de production
        if "line" in yaml_data:
            line_letter = str(yaml_data["line"]).lower()
        elif container_name and len(container_name) >= 5:
            line_letter = str(container_name[4]).lower()
        else:
            print(
                f"Impossible de déterminer la ligne de production pour {filepath.name} → Fichier ignoré."
            )
            continue      

        # Recherche du conteneur par la partition de ligne de production
        container_entity = next(
            (
                c
                for c in all_containers
                if f"line={line_letter}" in c.get("fullyQualifiedName", "").lower()
            ),
            None,
        )
        if not container_entity:
            print(f"Conteneur introuvable pour la ligne de production : {line_letter.upper()}")
            continue    

        # Correspondance des colonnes
        yaml_columns = yaml_data.get("columns", [])
        columns_map = {col["name"]: col for col in yaml_columns}

        data_model = container_entity.get("dataModel", {})
        existing_cols = data_model.get("columns", [])

        # Mise à jour des colonnes existantes
        for col in existing_cols:
            col_name = col.get("name")
            if col_name in columns_map:
                yaml_col = columns_map[col_name]

                # Description de colonne
                if yaml_col.get("description"):
                    col["description"] = yaml_col["description"]

                # Type de données (ex: STRING, INT, FLOAT, etc.)
                if yaml_col.get("dataType"):
                    col["dataType"] = yaml_col["dataType"].upper()

                # Tags de colonne (ex: TargetVariable)
                if yaml_col.get("tags"):
                    tags_list = []
                    for tag in yaml_col["tags"]:
                        tag_name = tag if isinstance(tag, str) else tag.get("tagFQN")
                        
                        # Conservation du FQN si un namespace/classification est déjà présent
                        if "." in tag_name:
                            tag_fqn = tag_name
                        else:
                            tag_fqn = f"TargetVariable.{tag_name}"

                        tags_list.append({
                            "tagFQN": tag_fqn,
                            "labelType": "Manual",
                            "state": "Confirmed",
                            "source": "Classification",
                        })
                    col["tags"] = tags_list

        # Traitement des colonnes YAML qui n'existaient pas encore dans l'ingestion S3
        existing_names = {c.get("name") for c in existing_cols}
        for col_name, yaml_col in columns_map.items():
            if col_name not in existing_names:
                tags_list = []
                if yaml_col.get("tags"):
                    for tag in yaml_col["tags"]:
                        tag_name = tag if isinstance(tag, str) else tag.get("tagFQN")

                        # Conservation du FQN si un namespace/classification est déjà présent
                        if "." in tag_name:
                            tag_fqn = tag_name
                        else:
                            tag_fqn = f"TargetVariable.{tag_name}"
                        
                        tags_list.append({
                            "tagFQN": tag_fqn,
                            "labelType": "Manual",
                            "state": "Confirmed",
                            "source": "Classification",
                        })

                existing_cols.append({
                    "name": col_name,
                    "dataType": yaml_col.get("dataType", "STRING").upper(),
                    "description": yaml_col.get("description", ""),
                    "tags": tags_list if tags_list else [],
                })

        data_model["columns"] = existing_cols

        # Construction du payload CreateContainer
        create_payload = {
            "name": container_entity.get("name"),
            "displayName": container_name,
            "description": yaml_data.get("description", ""),
            "service": container_entity.get("service", {}).get("name"),
            "dataModel": data_model,
        }

        # Conservation du conteneur parent s'il existe
        if container_entity.get("parent"):
            create_payload["parent"] = container_entity["parent"].get(
                "fullyQualifiedName"
            )

        # Sauvegarde de l'entité mise à jour
        put_url = f"{OPENMETADATA_HOST}/v1/containers"
        put_response = requests.put(
            put_url, headers=HEADERS, json=create_payload
        )

        if put_response.status_code in (200, 201):
            print(f"Fiche enrichie avec succès via REST : {container_name}")
        else:
            print(
                f"Erreur lors de la mise à jour ({put_response.status_code}) :"
                f" {put_response.text}"
            )


# ==========================================
# FONCTION DE TÂCHES
# ==========================================
def trigger_om_ingestion_from_ui():

  """Déclenche l'ingestion S3/MinIO via le pipeline configuré dans l'UI OpenMetadata."""
  
  url = f"{OPENMETADATA_HOST}/v1/services/ingestionPipelines/trigger/{PIPELINE_FQN}"
  

  response = requests.post(url, headers=HEADERS)

  if response.status_code not in (200, 201, 202):
    raise RuntimeError(
        f"Impossible de lancer le pipeline depuis l'UI ({response.status_code}) :"
        f" {response.text}"
    )

  print(f"Pipeline '{PIPELINE_FQN}' déclenché avec succès depuis l'UI OpenMetadata.")


# -------------------------------------------------------------------------
# Définition du DAG Airflow
# -------------------------------------------------------------------------
default_args = {
    "owner": "admin",
    "start_date": datetime(2026, 6, 1, tzinfo=timezone.utc),
}


with DAG(
    dag_id="minio_raw_ingestion_and_enrichment",
    default_args=default_args,
    description="Pipeline d'ingestion S3 MinIO_Raw et d'enrichissement fonctionnel",
    schedule_interval=None,    
    catchup=False,
    tags=["openmetadata", "minio", "ingestion"],
) as dag:

    # Ingestion automatique de la structure S3/MinIO
    task_s3_ingestion = PythonOperator(
        task_id="ingest_minio_s3_structure",        
        python_callable=trigger_om_ingestion_from_ui,
    )

    # Enrichissement fonctionnel via le script Python
    task_enrich_metadata = PythonOperator(
        task_id="enrich_containers_with_yaml",
        python_callable=run_metadata_enrichment,
    )

    # Dépendance : L'enrichissement s'exécute APRÈS l'ingestion S3
    task_s3_ingestion >> task_enrich_metadata

