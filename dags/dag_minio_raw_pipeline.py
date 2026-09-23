from datetime import datetime, timezone
import os
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from dotenv import load_dotenv
import requests
import urllib
import yaml


#Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration des accès OpenMetadata
OPENMETADATA_HOST = "http://openmetadata-server:8585/api"
JWT_TOKEN = os.getenv("JWT_ACCESS_TOKEN_USER_ADMIN")
YAML_METADATA_DIR = "/opt/airflow/dags/metadata/"
SERVICE_NAME = "MinIO_Raw"

def get_headers() -> dict:
    """
    Construit les headers d'authentification pour l'API OpenMetadata.
    Valide la présence du token uniquement au moment de l'exécution
    de la tâche (pas au parsing du DAG), pour ne pas casser le DAG
    pour tout le monde si la variable manque sur cet environnement.
    """
    if not JWT_TOKEN or not JWT_TOKEN.strip():
        raise RuntimeError(
            "JWT_ACCESS_TOKEN_USER_ADMIN est vide ou absent de "
            "l'environnement du worker Airflow."
        )
    return {
        "Authorization": f"Bearer {JWT_TOKEN.strip()}",
        "Content-Type": "application/json",
    }

def resolve_owner_id(owner_type: str, owner_name: str, headers: dict) -> str | None:
    entity_path = "users" if owner_type == "user" else "teams"
    resp = requests.get(
        f"{OPENMETADATA_HOST}/v1/{entity_path}/name/{owner_name}",
        headers=headers,
    )
    if resp.status_code == 200:
        return resp.json().get("id")
    return None

def build_data_model(yaml_columns: list) -> dict:
    """
    Construit le dataModel d'un conteneur 'line=X' à partir des colonnes
    de SA PROPRE fiche YAML uniquement.
    """
    columns = []
    for yaml_col in yaml_columns:
        col_name = yaml_col.get("name")
        if not col_name:
            continue
 
        # Formatage des tags
        tags_list = []
        if yaml_col.get("tags"):
            for tag in yaml_col["tags"]:
                tag_name = tag if isinstance(tag, str) else tag.get("tagFQN")
                tag_fqn = (
                    tag_name if "." in tag_name else f"TargetVariable.{tag_name}"
                )
                tags_list.append({
                    "tagFQN": tag_fqn,
                    "labelType": "Manual",
                    "state": "Confirmed",
                    "source": "Classification",
                })
 
        columns.append({
            "name": col_name,
            "dataType": yaml_col.get("dataType", "STRING").upper(),
            "description": yaml_col.get("description", ""),
            "tags": tags_list,
        })
 
    return {"isPartitioned": False, "columns": columns}

def upsert_line_container(yaml_data: dict, raw_container: dict, headers: dict) -> None:
    """Crée ou met à jour le conteneur enfant 'line=X' correspondant à une fiche YAML."""
    line = yaml_data.get("line")
    if not line:
        print(
            f"Fichier ignoré (champ 'line' manquant) : "
            f"{yaml_data.get('name', '?')}"
        )
        return
 
    container_name = f"line={line}"
 
    create_payload = {
        "name": container_name,
        "displayName": yaml_data.get("displayName") or yaml_data.get("name"),
        "description": yaml_data.get("description", "") or raw_container.get("description"),
        "service": raw_container.get("service", {}).get("name") or SERVICE_NAME,
        "parent": {"id": raw_container["id"], "type": "container"},
        "dataModel": build_data_model(yaml_data.get("columns", [])),
    }
 
    # --- GESTION SOUPLE DU PROPRIÉTAIRE (OWNER) ---
    yaml_owner = yaml_data.get("owner")

    if yaml_owner and isinstance(yaml_owner, dict) and yaml_owner.get("name"):
        owner_type = yaml_owner.get("type", "user")
        owner_name = yaml_owner.get("name")
        owner_id = resolve_owner_id(owner_type, owner_name, headers)
 
        if owner_id:
            create_payload["owners"] = [{
                "id": owner_id,
                "type": owner_type,
                "name": owner_name,
            }]
        else:
            print(f"Owner '{owner_name}' ({owner_type}) introuvable dans OpenMetadata")
    elif raw_container.get("owners"):
        # Conservation du propriétaire existant dans OpenMetadata s'il n'est pas spécifié dans le YAML
        create_payload["owners"] = raw_container.get("owners")
    # -----------------------------------------------
 
    put_url = f"{OPENMETADATA_HOST}/v1/containers"
    put_response = requests.put(put_url, headers=headers, json=create_payload)
 
    if put_response.status_code in (200, 201):
        print(f"Conteneur '{container_name}' enrichi avec succès dans OpenMetadata !")
    else:
        print(
            f"Erreur lors de la mise à jour de '{container_name}' "
            f"({put_response.status_code}) : {put_response.text}"
        )

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
        " fiche(s) de métadonnées (1 conteneur 'line=X' par fiche)..."
    )

    headers = get_headers()

    # Récupération des conteneurs existants pour trouver le FQN correspondant à la ligne de production
    res = requests.get(
        f"{OPENMETADATA_HOST}/v1/containers?limit=100&fields=dataModel,owners,service", headers=headers
    )
    if res.status_code != 200:
        raise RuntimeError(f"Erreur API ({res.status_code}) : {res.text}")

    all_containers = res.json().get("data", [])

    print(f"Nombre total de conteneurs récupérés : {len(all_containers)}")
    for c in all_containers:
        print(
            f"   - Name: '{c.get('name')}' | FQN:"
            f" '{c.get('fullyQualifiedName')}'"
        )


    # Ciblage du conteneur principal 'raw'
    raw_container = next(
        (
            c
            for c in all_containers
            if c.get("name") == "raw"
            or "MinIO_Raw.raw" in c.get("fullyQualifiedName", "")
        ),
        None,
    )

    if not raw_container:
        raise RuntimeError(
            "Le conteneur principal 'raw' est introuvable dans OpenMetadata."
        )    

    # Parcours de TOUS les fichiers YAML pour enrichir la structure globale : un conteneur enfant 'line=X' distinct par fichier YAML
    for filepath in yaml_files:
   
        with open(filepath, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)
        
        print(
            f"Traitement du fichier YAML : {filepath.name}"
            f" (ligne '{yaml_data.get('line')}')"
        ) 
        
        headers = get_headers()

        upsert_line_container(yaml_data, raw_container, headers)


# -------------------------------------------------------------------------
# Fonctions de Tâches
# -------------------------------------------------------------------------
def get_pipeline_id_from_service(service_name: str) -> str:
    """Récupère l'UUID du pipeline d'ingestion en interrogeant l'endpoint du Storage Service."""  

    headers = get_headers()

    # Interrogation du service de stockage
    # Le paramètre fields=pipelines demande à l'API d'inclure les pipelines rattachés
    url = f"{OPENMETADATA_HOST}/v1/services/storageServices/name/{service_name}?fields=pipelines"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise RuntimeError(
            f"Impossible de récupérer le service {service_name}"
            f" ({response.status_code}) : {response.text}"
        )

    data = response.json()
    # 'pipelines' contient la liste des entités/références rattachées au service
    ingestion_pipelines = data.get("pipelines", [])

    if not ingestion_pipelines:
        raise ValueError(
            f"Aucun pipeline d'ingestion n'est rattaché au service '{service_name}'."
        )

    # Récupération de l'UUID du premier pipeline d'ingestion
    pipeline_id = ingestion_pipelines[0].get("id")
    pipeline_name = ingestion_pipelines[0].get("name")
    print(
        f"Pipeline trouvé via le service {service_name} : {pipeline_name} (ID:"
        f" {pipeline_id})"
    )

    return pipeline_id


def wait_for_pipeline_completion(pipeline_id: str, timeout_sec=300):
    """Attend la fin de l'exécution du pipeline d'ingestion OpenMetadata."""

    headers = get_headers()

    status_url = f"{OPENMETADATA_HOST}/v1/services/ingestionPipelines/{pipeline_id}/pipelineStatus?limit=1"
    start_time = time.time()
    max_500_retries = 5
    consecutive_500_count = 0

    print("Ingestion déclenchée. Pause de 5 secondes avant le premier check...")
    time.sleep(5)

    while time.time() - start_time < timeout_sec:
        response = requests.get(status_url, headers=headers)

        if response.status_code == 200:
            statuses = response.json().get("data", [])
            if statuses:
                status = (
                    statuses[0].get("pipelineState") or statuses[0].get("status") or ""
                ).lower()
                print(f"Statut actuel du pipeline : {status}")                

                # Conditions de succès
                if status in ("success", "successful"):
                    print("Pipeline d'ingestion terminé avec succès !")
                    return True

                # Conditions d'échec
                elif status in ("failed", "failure", "partialsuccess"):
                    raise RuntimeError(
                        f"L'ingestion OpenMetadata a échoué avec le statut :"
                        f" {status}"
                    )
            else:
                print("Aucun statut trouvé pour le moment, attente...")

        elif response.status_code == 500 and "startTs" in response.text:
            consecutive_500_count += 1
            print(
                f"En attente de démarrage côté OpenMetadata ({consecutive_500_count}/{max_500_retries})..."
            )

            if consecutive_500_count >= max_500_retries:
                raise RuntimeError(
                    "Le runner d'ingestion OpenMetadata ne répond pas ou n'a pas"
                    " pu démarrer le job (startTs toujours Null)."
                )

        else:
            print(f"Erreur API OpenMetadata ({response.status_code}) : {response.text}")

        time.sleep(10)

    raise TimeoutError(
        f"Timeout après {timeout_sec}s : L'ingestion OpenMetadata n'est pas"
      " terminée."
    )

def trigger_om_ingestion_from_ui(**kwargs):

    """Déclenche l'ingestion S3/MinIO via le pipeline configuré dans l'UI OpenMetadata."""   


    # Récupération dynamique de l'UUID du pipeline d'ingestion via l'API du Storage Service
    pipeline_id = get_pipeline_id_from_service(SERVICE_NAME)


    # Appel POST direct sur l'endpoint /trigger/{UUID}
    trigger_url = f"{OPENMETADATA_HOST}/v1/services/ingestionPipelines/trigger/{pipeline_id}"  

    headers = get_headers()

    # Envoi d'un payload JSON pour forcer l'initialisation du startTs
    trigger_payload = {"startTs": int(time.time() * 1000)}    

    res_trigger = requests.post(trigger_url, headers=headers, json=trigger_payload)

    if res_trigger.status_code in (200, 201, 202):
        print(f"Pipeline (ID: {pipeline_id}) déclenché avec succès depuis l'UI OpenMetadata.")
    else:
        raise RuntimeError(
            f"Impossible de lancer le pipeline depuis l'UI ({res_trigger.status_code}) :"
            f" {res_trigger.text}"
        )

    # Transmission à la tâche suivante via XCom
    return pipeline_id


def wait_for_ingestion(**kwargs):
    ti = kwargs["ti"]
    pipeline_id = ti.xcom_pull(task_ids="task_s3_ingestion")
    wait_for_pipeline_completion(pipeline_id)


# -------------------------------------------------------------------------
# Définition du DAG Airflow
# -------------------------------------------------------------------------
default_args = {
    "owner": "admin",
    "start_date": datetime(2026, 6, 1, tzinfo=timezone.utc),
}


with DAG(
    "dag_minio_raw_ingestion_and_enrichment",
    default_args=default_args,
    description="Pipeline d'ingestion S3 MinIO_Raw et d'enrichissement fonctionnel",
    schedule_interval=None,    
    catchup=False,
    tags=["openmetadata", "minio", "ingestion"],
) as dag_minio_raw_ingestion_and_enrichment:

    # Ingestion automatique de la structure S3/MinIO
    task_s3_ingestion = PythonOperator(
        task_id="ingest_minio_s3_structure",        
        python_callable=trigger_om_ingestion_from_ui,
    )

    # Attente de la fin de l'ingestion
    task_wait_ingestion = PythonOperator(
        task_id="wait_for_ingestion_finished",
        python_callable=wait_for_ingestion,
    )

    # Enrichissement fonctionnel via le script Python
    task_enrich_metadata = PythonOperator(
        task_id="enrich_containers_with_yaml",
        python_callable=run_metadata_enrichment,
    )

    # Dépendance : L'enrichissement s'exécute APRÈS la fin de l'ingestion S3
    task_s3_ingestion >> task_wait_ingestion >> task_enrich_metadata

