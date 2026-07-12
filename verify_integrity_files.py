import os
from pathlib import Path
import boto3
import hashlib
from dotenv import load_dotenv


#Charger les variables d'environnement depuis .env
load_dotenv()

# Configurer le client MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
)

ARCHIVE_DIR = Path("./data/archive")

bucket = "raw"

files = {
    "lineA": "LineA_Stable_10K.csv",
    "lineB": "LineB_Flux.csv",
    "lineC": "LineC_Turbulent.csv",
    "lineD": "LineD_SpikeControl.csv",
    "lineE": "LineE_SmoothRun.csv"
}

def calculer_md5_local(file_path):
    # Calculer le hash MD5 d'un fichier local par morceaux : évite de surcharger la RAM
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

# Boucle de vérification
print("=== DÉBUT DE LA VÉRIFICATION DES FICHIERS CSV ===")

for line, file_name in files.items():

    # Construction du chemin exact (Object Key) sur MinIO
    minio_key = f"production_lines/{line}/{file_name}"

    print(f"\n{line[0].upper()+line[1:]} - vérification de : {file_name}")

    local_file = ARCHIVE_DIR/file_name
    
    if not os.path.exists(local_file):
        print(f"Erreur : Le fichier local '{file_name}' est introuvable.")
        continue

    try:
        # Récupérer l'ETag (MD5) sur MinIO
        response = s3.head_object(Bucket=bucket, Key=minio_key)
        # Nettoyage des guillemets retournés par S3
        minio_md5 = response['ETag'].strip('"')

        # Calculer le MD5 local
        local_md5 = calculer_md5_local(local_file)

        # Gérer le cas du Multipart upload (si le fichier est très gros)
        if '-' in minio_md5:
            print(f"Attention : Fichier uploadé en mode 'Multipart' ({minio_md5}).")
            print("La comparaison MD5 directe n'est pas possible.")
            continue

        # Comparaison
        if local_md5 == minio_md5:
            print(f"{file_name} : INTÈGRE (MD5: {local_md5})")
        else:
            print(f"{file_name} : CORROMPU ou DIFFÉRENT !")
            print(f"   -> Local : {local_md5}")
            print(f"   -> MinIO : {minio_md5}")

    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"{file_name} : N'existe pas sur le bucket {bucket}.")
        else:
            print(f"{file_name} : Erreur lors de l'accès à MinIO ({e})")

print("=== FIN DE LA VÉRIFICATION ===")