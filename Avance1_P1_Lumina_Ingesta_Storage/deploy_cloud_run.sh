#!/usr/bin/env bash
# ==============================================================================
# DESPLIEGUE SERVERLESS EN CLOUD RUN (ENTREGA 3 - P3)
# Autoridad de Tránsito Unificada "Lumina" — DataLandia Metropolitan
# Configurado con --min-instances=0 para costo $0.00 USD en reposo (Protección de Crédito $250)
# ==============================================================================
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-datalandia-lumina-p1}"
REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="lumina-command-center-p3"

echo "=================================================================="
echo " Desplegando Front-End y API BFF de Lumina en Google Cloud Run"
echo " Proyecto : ${PROJECT_ID}"
echo " Región   : ${REGION}"
echo " Servicio : ${SERVICE_NAME} (Scale-to-Zero activado: min-instances=0)"
echo "=================================================================="

gcloud config set project "${PROJECT_ID}"

gcloud run deploy "${SERVICE_NAME}" \
  --source . \
  --region "${REGION}" \
  --platform managed \
  --allow-unauthenticated \
  --min-instances 0 \
  --max-instances 2 \
  --memory 512Mi \
  --cpu 1 \
  --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID},GCP_REGION=${REGION}"

echo "=================================================================="
echo " [OK] Despliegue finalizado con éxito."
echo " Costo estimado en reposo: $0.00 USD / mes (Free Tier Cloud Run)."
echo "=================================================================="
