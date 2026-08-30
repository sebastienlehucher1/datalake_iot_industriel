#!/bin/sh
# =============================================================================
#  Initialisation MinIO — exécuté par le job one-shot `minio-init`.
#  Description :
#    - Crée les 4 buckets de l'architecture (raw, staging, curated, archive)
#    - Configure les policies IAM personnalisées par rôle
#    - Crée et associe les comptes applicatifs (data-analyst, data-engineer, admin)
#    - Applique les règles de cycle de vie ILM (purge staging, rétention/archivage raw).
#
#  Idempotent : relançable sans erreur. Les commandes susceptibles d'échouer
#  « parce que ça existe déjà » sont neutralisées par `|| true`.
#
#  Rappel modèle : une policy MinIO (IAM, JSON) liste des actions S3 sur des
#  ARN de buckets, puis est ATTACHÉE à un utilisateur. Les policies intégrées
#  (readonly/readwrite) portent sur TOUS les buckets -> on utilise des policies
#  personnalisées pour restreindre « selon bucket ».
# =============================================================================
set -eu

POLICIES="$(dirname "$0")/policies"

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# --- Buckets : un par couche ------------------------------------------------
for bucket in raw staging curated archive; do
  mc mb --ignore-existing "local/$bucket"
done

# --- Policies IAM personnalisées (droits par bucket) ------------------------
# `create` échoue si la policy existe déjà -> ignoré pour l'idempotence.
mc admin policy create local data-analyst  "$POLICIES/data-analyst.json"  2>/dev/null || true
mc admin policy create local data-engineer "$POLICIES/data-engineer.json" 2>/dev/null || true

# --- Comptes de service + attachement ---------------------------------------
# data-analyst : LECTURE SEULE sur curated/
mc admin user add local "$MINIO_ANALYST_USER" "$MINIO_ANALYST_PASSWORD" 2>/dev/null || true
mc admin policy attach local data-analyst --user "$MINIO_ANALYST_USER" 2>/dev/null || true

# data-engineer : LECTURE/ÉCRITURE sur raw/ + staging/ + curated/
mc admin user add local "$MINIO_ENGINEER_USER" "$MINIO_ENGINEER_PASSWORD" 2>/dev/null || true
mc admin policy attach local data-engineer --user "$MINIO_ENGINEER_USER" 2>/dev/null || true

# admin : TOUS DROITS (policy intégrée consoleAdmin) — root MinIO l'est déjà
mc admin user add local "$MINIO_ADMIN_USER" "$MINIO_ADMIN_PASSWORD" 2>/dev/null || true
mc admin policy attach local consoleAdmin --user "$MINIO_ADMIN_USER" 2>/dev/null || true

# --- Configuration ILM (Life Cycle Rules) -----------------------------------
# raw : archivage 180 jours (WARM-ARCHIVE), expiration 730 jours (2 ans) via le fichier JSON
mc ilm import local/raw "$POLICIES/raw-ilm.json" 2>/dev/null || true

# staging : purge automatique après 30 jours (zone tampon)
mc ilm rule add --expiry-days 30 local/staging 2>/dev/null || true

echo "MinIO prêt : buckets raw/staging/curated/archive + comptes data-analyst (RO curated),"
echo "             data-engineer (RW raw/staging/curated), admin (tous droits) +"
echo "             règles ILM configurées."