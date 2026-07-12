import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import os
from pathlib import Path
import boto3
from dotenv import load_dotenv
import requests


#Charger les variables d'environnement depuis .env
load_dotenv()

def upload_files():

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
    )

    urls = [
        "https://zenodo.org/records/15277168/files/LineA_Stable_10K.csv",
        "https://zenodo.org/records/15277168/files/LineB_Flux.csv",
        "https://zenodo.org/records/15277168/files/LineC_Turbulent.csv",
        "https://zenodo.org/records/15277168/files/LineD_SpikeControl.csv",
        "https://zenodo.org/records/15277168/files/LineE_SmoothRun.csv",
    ]

    INCOMING_DIR = Path("/opt/airflow/data/incoming")

    os.makedirs(INCOMING_DIR, exist_ok=True)    

    for url in urls:
        file_name = url.split("/")[-1]
        file_path = os.path.join(INCOMING_DIR, file_name)

        response = requests.get(url)
        response.raise_for_status()

        with open(file_path, "wb") as f:
            f.write(response.content)

        print(f"Téléchargé : {file_path}")
    

    ARCHIVE_DIR = Path("/opt/airflow/data/archive") / datetime.now().strftime("%Y%m")

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    bucket = "raw"

    files = {
        "lineA": "LineA_Stable_10K.csv",
        "lineB": "LineB_Flux.csv",
        "lineC": "LineC_Turbulent.csv",
        "lineD": "LineD_SpikeControl.csv",
        "lineE": "LineE_SmoothRun.csv"
    }

    for line, file_name in files.items():
        key = f"year={datetime.now().strftime('%Y')}/month={datetime.now().strftime('%m')}/line={line[-1]}/{Path(file_name).name}"

        local_file = INCOMING_DIR / file_name

        if not local_file.exists():
            print(f"Fichier absent : {local_file}")
            continue
        
        s3.upload_file(
            Filename=str(local_file),
            Bucket=bucket,
            Key=key
        )

        print(f"Upload OK : s3://{bucket}/{key}")

        archive_file = ARCHIVE_DIR / file_name

        shutil.move(
            str(local_file),
            str(archive_file)
        )

        print(f"{file_name} archivé en local")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 6, 1, tzinfo=timezone.utc),
}


with DAG(
    "dag_upload_CSV_files",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
) as dag_upload_CSV_files:
    upload_csv_files = PythonOperator(
        task_id="upload_csv_files",
        python_callable=upload_files,
    )