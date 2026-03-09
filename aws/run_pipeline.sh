#!/usr/bin/env bash
# aws/run_pipeline.sh
#
# Submit the 3-step weighted webgraph pipeline to AWS EMR Serverless.
#
# ── Pipeline steps ──────────────────────────────────────────────────────────
#   1. expand_domains.py     – find backlinkers via CC host graph
#   2. get_wat_index.py      – get WAT record coordinates from CC columnar index
#   3. weighted_extract_links.py – extract weighted link counts from WAT records
#
# ── Prerequisites ────────────────────────────────────────────────────────────
#   1. EMR Serverless Spark application created in AWS console/CLI
#   2. IAM role with:
#        emr-serverless:StartJobRun, GetJobRun
#        s3:GetObject, PutObject on s3://cc-pyspark/*
#        s3:GetObject on s3://commoncrawl/* (requester-pays)
#   3. Python venv packaged:  cd aws && bash create_emr_env.sh
#      then uploaded:         aws s3 cp aws/pyspark_venv.tar.gz s3://cc-pyspark/emr/
#   4. Input CSV uploaded:    aws s3 cp domains.csv s3://cc-pyspark/domains.csv
#
# ── Required env vars ────────────────────────────────────────────────────────
#   EMR_APPLICATION_ID   from your EMR Serverless application
#   JOB_ROLE_ARN         IAM role ARN
#
# ── Testing options ──────────────────────────────────────────────────────────
#   STEPS=1            run only step 1 (domain expansion)
#   STEPS=1,2          run only steps 1 and 2
#   STEPS=1,2,3        run all steps (default)
#   MAX_WAT_PER_HOST=N cap WAT records per host in step 2 (cost control)
#
# ── Usage ────────────────────────────────────────────────────────────────────
#   # Full pipeline
#   EMR_APPLICATION_ID=00abc EMR_JOB_ROLE_ARN=arn:... bash aws/run_pipeline.sh
#
#   # Test: domain expansion only
#   STEPS=1 EMR_APPLICATION_ID=00abc JOB_ROLE_ARN=arn:... bash aws/run_pipeline.sh
#
#   # Test: expansion + WAT index, capped at 100 WAT records per host
#   STEPS=1,2 MAX_WAT_PER_HOST=100 EMR_APPLICATION_ID=00abc JOB_ROLE_ARN=arn:... bash aws/run_pipeline.sh

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
S3_BUCKET="${S3_BUCKET:-cc-pyspark}"

# CC webgraph crawl name — format: cc-main-YYYY-mon-mon-mon
# Browse available crawls: s3://commoncrawl/projects/hyperlinkgraph/
WEBGRAPH_CRAWL="${WEBGRAPH_CRAWL:-cc-main-2024-aug-sep-oct}"

# CC data crawl name — format: CC-MAIN-YYYY-WW (used to filter CC columnar index)
# The webgraph crawl above covers multiple CC-MAIN crawls; pick the most recent one.
# Note: CC-MAIN crawl IDs skip some weeks. For cc-main-2024-aug-sep-oct the options are:
#   CC-MAIN-2024-33 (Aug), CC-MAIN-2024-38 (Sep), CC-MAIN-2024-42 (Oct)
DATA_CRAWL="${DATA_CRAWL:-CC-MAIN-2024-38}"

STEPS="${STEPS:-1,2,3}"
MAX_WAT_PER_HOST="${MAX_WAT_PER_HOST:-}"

TARGET_DOMAINS_S3="${TARGET_DOMAINS_S3:-s3://${S3_BUCKET}/domains.csv}"

BASE="s3://${S3_BUCKET}/pipeline/${WEBGRAPH_CRAWL}"
WEBGRAPH_PREFIX="s3://commoncrawl/projects/hyperlinkgraph/${WEBGRAPH_CRAWL}/host"

CODE_S3="${BASE}/code"
EXPANDED_DOMAINS_S3="${EXPANDED_DOMAINS_S3:-${BASE}/expanded_domains}"
WAT_INDEX_S3="${WAT_INDEX_S3:-${BASE}/wat_index}"
WAREHOUSE_S3="${BASE}/warehouse"
VENV_S3="${VENV_S3:-s3://${S3_BUCKET}/emr/pyspark_venv.tar.gz}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
PIPELINE_DIR="${REPO_DIR}/cc/pipeline"
CC_OFFICIAL="${CC_OFFICIAL:-${REPO_DIR}/cc-pyspark-official/cc-pyspark-official}"

run_step() { echo "${STEPS}" | tr ',' '\n' | grep -q "^${1}$"; }

# ── Package and upload code ────────────────────────────────────────────────────
echo "=== Packaging and uploading code ==="

TMPDIR_LOCAL="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_LOCAL"' EXIT

zip -j "${TMPDIR_LOCAL}/cc_pyspark.zip" \
    "${CC_OFFICIAL}/sparkcc.py" \
    "${CC_OFFICIAL}/json_importer.py"

aws s3 cp "${TMPDIR_LOCAL}/cc_pyspark.zip" "${CODE_S3}/cc_pyspark.zip"

for script in expand_domains get_wat_index weighted_extract_links; do
    aws s3 cp "${PIPELINE_DIR}/${script}.py" "${CODE_S3}/${script}.py"
done

echo "Code uploaded to ${CODE_S3}/"

# ── Common Spark parameters ────────────────────────────────────────────────────
SPARK_PARAMS="\
--conf spark.archives=${VENV_S3}#environment \
--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python \
--conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
--conf spark.driver.cores=4 \
--conf spark.driver.memory=16g \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=16g \
--conf spark.sql.warehouse.dir=${WAREHOUSE_S3} \
--py-files ${CODE_S3}/cc_pyspark.zip"

# ── Helpers ────────────────────────────────────────────────────────────────────
submit_job() {
    local name="$1" entry_point="$2" entry_args="$3"
    aws emr-serverless start-job-run \
        --application-id "${EMR_APPLICATION_ID}" \
        --execution-role-arn "${JOB_ROLE_ARN}" \
        --name "${name}" \
        --job-driver "{
            \"sparkSubmit\": {
                \"entryPoint\": \"${entry_point}\",
                \"entryPointArguments\": ${entry_args},
                \"sparkSubmitParameters\": \"${SPARK_PARAMS}\"
            }
        }" \
        --query 'jobRunId' --output text
}

wait_for_job() {
    local job_id="$1"
    echo "Waiting for ${job_id}..."
    while true; do
        state=$(aws emr-serverless get-job-run \
            --application-id "${EMR_APPLICATION_ID}" \
            --job-run-id "${job_id}" \
            --query 'jobRun.state' --output text)
        printf '  [%s] %s\n' "$(date -u +%H:%M:%S)" "${state}"
        case "${state}" in
            SUCCESS) return 0 ;;
            FAILED|CANCELLED|CANCELLING)
                echo "ERROR: job ${job_id} ended with state ${state}"
                aws emr-serverless get-job-run \
                    --application-id "${EMR_APPLICATION_ID}" \
                    --job-run-id "${job_id}" \
                    --query 'jobRun.stateDetails' --output text
                exit 1 ;;
        esac
        sleep 30
    done
}

# ── Step 1: Domain expansion ───────────────────────────────────────────────────
if run_step 1; then
    echo ""
    echo "=== Step 1: Domain expansion (backlinkers via ${WEBGRAPH_CRAWL}) ==="
    ARGS="[
        \"--target_domains\",   \"${TARGET_DOMAINS_S3}\",
        \"--webgraph_prefix\",  \"${WEBGRAPH_PREFIX}\",
        \"--output\",           \"${EXPANDED_DOMAINS_S3}\"
    ]"
    JOB1=$(submit_job "1-expand-domains" "${CODE_S3}/expand_domains.py" "${ARGS}")
    echo "Job ID: ${JOB1}"
    wait_for_job "${JOB1}"
fi

# ── Step 2: WAT index lookup ───────────────────────────────────────────────────
if run_step 2; then
    echo ""
    echo "=== Step 2: WAT index lookup (crawl=${DATA_CRAWL}) ==="
    ARGS="[
        \"--expanded_domains\", \"${EXPANDED_DOMAINS_S3}\",
        \"--crawl\",            \"${DATA_CRAWL}\",
        \"--output\",           \"${WAT_INDEX_S3}\""
    [[ -n "${MAX_WAT_PER_HOST}" ]] && ARGS+=",
        \"--max_records_per_host\", \"${MAX_WAT_PER_HOST}\""
    ARGS+="]"
    JOB2=$(submit_job "2-get-wat-index" "${CODE_S3}/get_wat_index.py" "${ARGS}")
    echo "Job ID: ${JOB2}"
    wait_for_job "${JOB2}"
fi

# ── Step 3: Weighted link extraction ──────────────────────────────────────────
if run_step 3; then
    echo ""
    echo "=== Step 3: Weighted link extraction ==="

    # num_input_partitions: 1 per MB of WAT index parquet (min 10).
    # Avoids thousands of empty partitions on small runs; scales with data volume.
    WAT_SIZE_BYTES=$(aws s3 ls --recursive "${WAT_INDEX_S3}/" \
        | grep '\.parquet' | awk '{sum+=$3} END {print sum+0}')
    WAT_SIZE_MB=$(( (WAT_SIZE_BYTES + 1048575) / 1048576 ))
    NUM_INPUT_PARTITIONS=$(( WAT_SIZE_MB > 10 ? WAT_SIZE_MB : 10 ))

    # num_output_partitions: 2 per target domain (min 1).
    # Output rows ≈ unique_sources × num_targets, so target count is the key
    # scaling axis. 2× gives headroom without producing many tiny files.
    NUM_TARGETS=$(aws s3 cp "${TARGET_DOMAINS_S3}" - 2>/dev/null \
        | grep -c '[^[:space:]]' || echo 1)
    NUM_OUTPUT_PARTITIONS=$(( NUM_TARGETS * 2 > 1 ? NUM_TARGETS * 2 : 1 ))

    echo "WAT index: ${WAT_SIZE_MB} MB → num_input_partitions=${NUM_INPUT_PARTITIONS}"
    echo "Target domains: ${NUM_TARGETS} → num_output_partitions=${NUM_OUTPUT_PARTITIONS}"

    ARGS="[
        \"${WAT_INDEX_S3}\",
        \"weighted_links\",
        \"--input_table_format\",    \"parquet\",
        \"--input_base_url\",        \"s3://commoncrawl/\",
        \"--num_input_partitions\",  \"${NUM_INPUT_PARTITIONS}\",
        \"--num_output_partitions\", \"${NUM_OUTPUT_PARTITIONS}\",
        \"--target_domains\",        \"${TARGET_DOMAINS_S3}\"
    ]"
    JOB3=$(submit_job "3-weighted-extract" "${CODE_S3}/weighted_extract_links.py" "${ARGS}")
    echo "Job ID: ${JOB3}"
    wait_for_job "${JOB3}"
fi

echo ""
echo "=== Done. Output at ${WAREHOUSE_S3}/weighted_links/ ==="
echo "Sync locally:  aws s3 sync ${WAREHOUSE_S3}/weighted_links/ cc/spark-warehouse/weighted_links/"
