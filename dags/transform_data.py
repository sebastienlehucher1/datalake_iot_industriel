from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import time
import os
import boto3
import io
import pandas as pd
from dotenv import load_dotenv


#Charger les variables d'environnement depuis .env
load_dotenv()


RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"

# Nombre de lignes par batch
CHUNK_SIZE = 1000
# Pause entre chaque chunk pour simuler un flux réel
SIMULATE_DELAY_SECONDS = 2 

COLUMN_MAPPING = {
    "Temperature": "temperature",
    "temperature": "temperature",
    "Pressure": "pressure",
    "pressure": "pressure",
    "Elapsed_time": "elapsed_time",
    "elapsed_time": "elapsed_time",
    "timestamp": "timestamp",
    "label": "label",
}

def transform_dataframe(df):

    df: pd.DataFrame = df

    # Harmoniser les noms de colonnes
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # Vérifier la colonne timestamp
    if "timestamp" not in df.columns:
        raise ValueError(f"Colonne 'timestamp' absente")

    # Normaliser timestamp ISO8601 UTC
    df["timestamp"] = (
        pd.to_datetime(
            df["timestamp"],
            errors="coerce",
            utc=True
        )
        .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    # Standardiser les colonnes
    standard_columns = [
        "timestamp",
        "temperature",
        "pressure",
        "elapsed_time",
        "label"
    ]

    # Conserver uniquement les colonnes présentes
    final_columns = [
        col for col in standard_columns
        if col in df.columns
    ]

    return df[final_columns]


def transform_file_in_chunks(s3, key, chunk_size=CHUNK_SIZE):
    """
    Lit un CSV depuis raw par chunks et dépose chaque batch transformé
    dans staging, en simulant un flux réel (batch par batch).
    """

    try:
        # Lire les fichiers CSV depuis raw
        response = s3.get_object(
            Bucket=RAW_BUCKET,
            Key=key
        )

        reader = pd.read_csv(
            response["Body"],
            chunksize=chunk_size,
        )

        base_name = key.rsplit(".csv", 1)[0]

        for i, chunk in enumerate(reader, start=1):
            chunk = transform_dataframe(chunk)

            # Converver en fichier CSV
            csv_buffer = io.StringIO()
            chunk.to_csv(csv_buffer, index=False)

            chunk_key = f"{base_name}_batch_{i:03d}.csv"

            # Écrire dans staging
            s3.put_object(
                Bucket=STAGING_BUCKET,
                Key=chunk_key,
                Body=csv_buffer.getvalue()
            )

            print(f"Batch {i} ({len(chunk)} lignes) déposé dans {STAGING_BUCKET}/{chunk_key}")

            # Simule l'arrivée progressive des données
            time.sleep(SIMULATE_DELAY_SECONDS)
    
    except Exception as e:
        print(
            f"Erreur lors du traitement du batch {i} de "
            f"{key} : {str(e)}"
        )

def process_raw_to_staging():
    """
    Lit tous les CSV du bucket raw,
    harmonise les colonnes,
    normalise les timestamps,
    puis dépose les fichiers transformés dans le bucket staging.
    """

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=RAW_BUCKET,
        Prefix=f"year={datetime.now().strftime('%Y')}/month={datetime.now().strftime('%m')}/"
    ):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if not key.endswith(".csv"):
                continue

            print(f"Traitement du fichier (par chunks) : {key}")

            try:
                # Traitement en batch pour simuler un flux réel
                transform_file_in_chunks(s3, key)
                                  
            except Exception as e:
                print(
                    f"Erreur lors du traitement de "
                    f"{key} : {str(e)}"
                )

            


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 6, 1, tzinfo=timezone.utc),
}


with DAG(
    "dag_transform_CSV_files_data",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag_transform_CSV_files_data:
    transform_CSV_files_data = PythonOperator(
        task_id="transform_CSV_files_data",
        python_callable=process_raw_to_staging,
    )