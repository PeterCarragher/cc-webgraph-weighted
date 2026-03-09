#!/usr/bin/env bash
# aws/run_local.sh
#
# Run the pipeline locally using spark-submit (no EMR Serverless required).
# Useful for verifying logic and inspecting intermediate outputs cheaply.
#
# Steps 1 and 2 read from s3://commoncrawl (requester-pays) directly.
# Step 3 also reads from s3://commoncrawl, fetching WAT records by byte range.
# AWS credentials must be configured and allow requester-pays access.
#
# ── Testing options ──────────────────────────────────────────────────────────
#   STEPS=1            run only domain expansion
#   STEPS=1,2          run expansion + WAT index lookup (no WARC fetching)
#   STEPS=1,2,3        run full pipeline (default)
#   MAX_WAT_PER_HOST=N cap WAT records per host (strongly recommended locally)
#
# ── Usage ────────────────────────────────────────────────────────────────────
#   # Cheapest test: just domain expansion, output written locally
#   STEPS=1 bash aws/run_local.sh
#
#   # Check how many WAT records would be fetched (step 2), capped at 5/host
#   STEPS=1,2 MAX_WAT_PER_HOST=5 bash aws/run_local.sh
#
#   # Full local pipeline with 5 WAT records per host (tiny weighted graph)
#   STEPS=1,2,3 MAX_WAT_PER_HOST=5 bash aws/run_local.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
PIPELINE_DIR="${REPO_DIR}/cc/pipeline"
CC_OFFICIAL="${CC_OFFICIAL:-${REPO_DIR}/cc-pyspark-official/cc-pyspark-official}"

# ── Configuration ─────────────────────────────────────────────────────────────
S3_BUCKET="${S3_BUCKET:-cc-pyspark}"
WEBGRAPH_CRAWL="${WEBGRAPH_CRAWL:-cc-main-2024-aug-sep-oct}"
DATA_CRAWL="${DATA_CRAWL:-CC-MAIN-2024-38}"
STEPS="${STEPS:-1,2,3}"
MAX_WAT_PER_HOST="${MAX_WAT_PER_HOST:-}"

TARGET_DOMAINS="${TARGET_DOMAINS:-s3://${S3_BUCKET}/domains.csv}"
WEBGRAPH_PREFIX="s3://commoncrawl/projects/hyperlinkgraph/${WEBGRAPH_CRAWL}/host"

OUT_DIR="${REPO_DIR}/cc/pipeline_output/${WEBGRAPH_CRAWL}"
EXPANDED_DOMAINS="${OUT_DIR}/expanded_domains"
WAT_INDEX="${OUT_DIR}/wat_index"
WAREHOUSE="${OUT_DIR}/warehouse"

mkdir -p "${OUT_DIR}"

PYTHONPATH="${CC_OFFICIAL}:${PIPELINE_DIR}"
export PYTHONPATH

run_step() { echo "${STEPS}" | tr ',' '\n' | grep -q "^${1}$"; }

spark_submit() {
    spark-submit \
        --master "local[*]" \
        --driver-memory 8g \
        --conf "spark.sql.warehouse.dir=${WAREHOUSE}" \
        --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
        --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
        "$@"
}

# ── Step 1: Domain expansion ───────────────────────────────────────────────────
if run_step 1; then
    echo "=== Step 1: Domain expansion ==="
    spark_submit "${PIPELINE_DIR}/expand_domains.py" \
        --target_domains  "${TARGET_DOMAINS}" \
        --webgraph_prefix "${WEBGRAPH_PREFIX}" \
        --output          "${EXPANDED_DOMAINS}"
    echo "Expanded domains written to: ${EXPANDED_DOMAINS}"
fi

# ── Step 2: WAT index lookup ───────────────────────────────────────────────────
if run_step 2; then
    echo "=== Step 2: WAT index lookup ==="
    STEP2_ARGS=(
        --expanded_domains "${EXPANDED_DOMAINS}"
        --cc_index         "s3://commoncrawl/cc-index/table/cc-main/warc/"
        --crawl            "${DATA_CRAWL}"
        --output           "${WAT_INDEX}"
    )
    [[ -n "${MAX_WAT_PER_HOST}" ]] && STEP2_ARGS+=(--max_records_per_host "${MAX_WAT_PER_HOST}")
    spark_submit "${PIPELINE_DIR}/get_wat_index.py" "${STEP2_ARGS[@]}"

    echo "WAT index written to: ${WAT_INDEX}"
    echo ""
    echo "Record count:"
    python3 -c "
import sys; sys.path.insert(0, '${CC_OFFICIAL}')
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('count').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
df = spark.read.parquet('${WAT_INDEX}')
print(f'  {df.count()} WAT records to process in step 3')
spark.stop()
" 2>/dev/null
fi

# ── Step 3: Weighted link extraction ──────────────────────────────────────────
if run_step 3; then
    echo "=== Step 3: Weighted link extraction ==="
    spark_submit "${PIPELINE_DIR}/weighted_extract_links.py" \
        "${WAT_INDEX}" \
        "weighted_links" \
        --input_table_format parquet \
        --input_base_url     "s3://commoncrawl/" \
        --num_input_partitions  50 \
        --num_output_partitions 4 \
        --target_domains        "${TARGET_DOMAINS}"
    echo "Output table: ${WAREHOUSE}/weighted_links/"
fi

echo ""
echo "=== Done. Outputs in ${OUT_DIR} ==="
