#!/bin/bash
# =============================================================================
# build_and_push.sh
# Construye la imagen Docker y la sube a Amazon ECR (us-east-1)
#
# REQUISITOS PREVIOS:
#   - Docker instalado y corriendo
#   - AWS CLI instalado y configurado (aws configure)
#   - Permisos IAM: ecr:CreateRepository, ecr:GetAuthorizationToken,
#                   ecr:BatchCheckLayerAvailability, ecr:PutImage, etc.
#
# USO:
#   chmod +x build_and_push.sh
#   ./build_and_push.sh
# =============================================================================

set -e  # detener el script si cualquier comando falla

# =============================================================================
# CONFIGURACIÓN — ajusta estos valores antes de ejecutar
# =============================================================================

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION="eu-south-2"
ECR_REPO_NAME="itl-0004-itx-dev-rep-visa-xch-rt"
IMAGE_TAG="latest"

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

# =============================================================================
# PASO 1: Crear el repositorio en ECR si no existe
# =============================================================================

echo "1.1. Verificando repositorio ECR: ${ECR_REPO_NAME}..."
aws ecr describe-repositories \
    --repository-names "${ECR_REPO_NAME}" \
    --region "${AWS_REGION}" > /dev/null 2>&1 \
|| aws ecr create-repository \
    --repository-name "${ECR_REPO_NAME}" \
    --region "${AWS_REGION}"
echo "1.2. Repositorio ECR listo: ${ECR_URI}"

# =============================================================================
# PASO 2: Autenticarse en ECR
# =============================================================================

echo "2.1. Autenticando Docker en ECR..."
aws ecr get-login-password \
    --region "${AWS_REGION}" \
| docker login \
    --username AWS \
    --password-stdin \
    "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "2.2 Autenticación exitosa"

# =============================================================================
# PASO 3: Build de la imagen
# =============================================================================

echo "🐳 Construyendo imagen Docker..."
docker buildx build \
    --platform linux/amd64 \
    --provenance=false \
    -t "${ECR_REPO_NAME}:${IMAGE_TAG}" \
    .

echo "✅ Imagen construida: ${ECR_REPO_NAME}:${IMAGE_TAG}"

# =============================================================================
# PASO 4: Taggear y subir a ECR
# =============================================================================

echo "🏷️  Taggeando imagen..."
docker tag \
    "${ECR_REPO_NAME}:${IMAGE_TAG}" \
    "${ECR_URI}:${IMAGE_TAG}"

echo "🚀 Subiendo imagen a ECR..."
docker push "${ECR_URI}:${IMAGE_TAG}"

echo ""
echo "=============================================="
echo "✅ Imagen subida exitosamente a ECR"
echo ""
echo "URI de la imagen para Lambda:"
echo "${ECR_URI}:${IMAGE_TAG}"
echo ""
echo "Próximo paso:"
echo "  AWS Console → Lambda → Tu función"
echo "  → 'Image' → 'Deploy new image'"
echo "  → Pegar el URI de arriba"
echo "=============================================="

# =============================================================================
# PASO 5: Actualizar Lambda con la nueva imagen
# =============================================================================

FUNCTION_NAME="visa-exchange-rates-scraper"

echo "🔄 Actualizando Lambda con la nueva imagen..."
aws lambda update-function-code \
  --function-name ${FUNCTION_NAME} \
  --image-uri ${ECR_URI}:${IMAGE_TAG} \
  --region ${AWS_REGION}

echo "⏳ Esperando que Lambda termine de actualizarse..."
aws lambda wait function-updated \
  --function-name ${FUNCTION_NAME} \
  --region ${AWS_REGION}

echo ""
echo "=============================================="
echo "✅ Lambda actualizada y lista para probar"
echo "=============================================="