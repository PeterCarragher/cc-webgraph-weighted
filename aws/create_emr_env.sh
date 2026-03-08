#!/usr/bin/env bash
# aws/create_emr_env.sh
#
# Package a Python environment for EMR Serverless using conda-pack.
# The resulting pyspark_venv.tar.gz is uploaded to S3 and referenced
# by spark.archives in run_pipeline.sh.
#
# Requires: conda + conda-pack (conda install -c conda-forge conda-pack)
#
# Usage:
#   bash aws/create_emr_env.sh
#   # then verify the upload:
#   aws s3 ls s3://cc-pyspark/emr/pyspark_venv.tar.gz

set -euo pipefail

S3_BUCKET="${S3_BUCKET:-cc-pyspark}"
ENV_NAME="emr_pyspark_env"

echo "=== Creating conda environment: ${ENV_NAME} (Python 3.9) ==="
conda create -n "${ENV_NAME}" python=3.9 -y

echo "=== Installing packages ==="
conda run -n "${ENV_NAME}" pip install requests warcio idna boto3 botocore

echo "=== Installing conda-pack ==="
conda install -n "${ENV_NAME}" -c conda-forge conda-pack -y

echo "=== Packaging with conda-pack ==="
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
conda run -n "${ENV_NAME}" conda-pack -o "${TMPDIR}/pyspark_venv.tar.gz"

echo "Archive size: $(du -sh "${TMPDIR}/pyspark_venv.tar.gz" | cut -f1)"

echo "=== Uploading to s3://${S3_BUCKET}/emr/pyspark_venv.tar.gz ==="
aws s3 cp "${TMPDIR}/pyspark_venv.tar.gz" "s3://${S3_BUCKET}/emr/pyspark_venv.tar.gz"

echo ""
echo "Done. Venv uploaded to s3://${S3_BUCKET}/emr/pyspark_venv.tar.gz"
echo "Run the pipeline with:"
echo "  VENV_S3=s3://${S3_BUCKET}/emr/pyspark_venv.tar.gz bash aws/run_pipeline.sh"
