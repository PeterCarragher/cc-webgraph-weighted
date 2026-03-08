# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Research project that builds a **weighted webgraph** from Common Crawl data to score websites (particularly podcasts) for media reliability. The core idea: expand a list of target news domains using the CC host graph, fetch pre-parsed WAT metadata records for those domains' neighborhood, count outgoing links between hosts, and use the resulting weighted edges to compute an "outlink unreliability score" for each domain.

## Repository Structure

- `cc-pyspark/` — Git submodule (`PeterCarragher/cc-pyspark`). An earlier fork used for local prototyping. The pipeline now depends on the **official** cc-pyspark (see below).
- `cc-pyspark-official/cc-pyspark-official/` — Local clone of the official [`commoncrawl/cc-pyspark`](https://github.com/commoncrawl/cc-pyspark). Pipeline scripts import from here (`sparkcc.py`, `json_importer.py`).
- `cc/pipeline/` — **The EMR Serverless pipeline** (three Spark jobs, see below).
- `cc/*.ipynb` — Analysis notebooks that consume the pipeline output from `cc/spark-warehouse/`.
- `aws/` — EMR environment setup and pipeline submission scripts.

## Pipeline (cc/pipeline/)

Three sequential Spark jobs. Run via `aws/run_pipeline.sh`.

### Step 1 — `expand_domains.py`
**Input**: target domains CSV (column `domain`, e.g. `nytimes.com`) + CC host graph on S3
**Output**: Parquet with column `host_reversed` (SURT format, e.g. `com.nytimes`)

Finds all **backlinkers** of target domains (domains that link TO the targets) using the CC host graph. Outlinkers not included yet.

**CC host graph format** (verified):
- S3 prefix: `s3://commoncrawl/projects/hyperlinkgraph/<crawl>/host/`
- Crawl naming: `cc-main-YYYY-mon-mon-mon` (e.g. `cc-main-2024-aug-sep-oct`) — not `CC-MAIN-YYYY-WW`
- `vertices/part-*.txt.gz`: `<id>\t<host_reversed>`, multi-part gzipped text
- `edges/part-*.txt.gz`: `<src_id>\t<tgt_id>` edge pairs, multi-part gzipped text
- Edge `src→tgt` means host `src` links TO host `tgt`; bucket is requester-pays
- `--webgraph_prefix` arg takes the `host/` directory (script appends `/vertices` and `/edges`)

### Step 2 — `get_wat_index.py`
**Input**: expanded domains Parquet + CC columnar index
**Output**: Parquet with `url, warc_filename, warc_record_offset, warc_record_length`

Queries the CC columnar index (`s3://commoncrawl/cc-index/table/cc-main/warc/`) for WAT records (`subset = 'wat'`) from expanded hosts. WAT files contain pre-parsed JSON link metadata — much smaller and faster to process than raw WARC HTML.

Host matching normalizes `www.` stripping on both sides (webgraph strips `www.` via `get_surt_host`; index side strips it via `regexp_replace`).

### Step 3 — `weighted_extract_links.py`
**Input**: WAT record coordinates from Step 2
**Output**: Spark table `weighted_links` with schema `(s STRING, t STRING, count LONG)`

Extends `CCIndexWarcSparkJob` (official cc-pyspark). Fetches WAT records from S3 by byte range, parses the JSON link metadata, and **counts** all outgoing href links per `(source_host_surt, target_host_surt)` pair (unlike the standard `ExtractHostLinksJob` which deduplicates to a binary edge). Both `s` and `t` are in SURT format (`com.example`). Self-links are dropped.

### Key design choices
- Uses **WAT** (not WARC) records: WAT JSON is pre-parsed and ~10× smaller than raw HTML.
- `--input_base_url s3://commoncrawl/` in Step 3: `warc_filename` in the CC index is a relative path; this prefix resolves it to the correct S3 URI.
- Step 2 supports `--max_records_per_host N` for cost-capped test runs.

## Running the Pipeline

S3 bucket: `cc-pyspark`. Input CSV: `s3://cc-pyspark/domains.csv` (10 test domains, column `domain`).

```bash
# Package and upload venv (once)
cd aws && bash create_emr_env.sh
aws s3 cp pyspark_venv.tar.gz s3://cc-pyspark/emr/

# Option A – Local spark-submit (no EMR cost, good for logic testing)
STEPS=1   bash aws/run_local.sh                        # domain expansion only
STEPS=1,2 MAX_WAT_PER_HOST=5 bash aws/run_local.sh    # + WAT index, capped
STEPS=1,2,3 MAX_WAT_PER_HOST=5 bash aws/run_local.sh  # full pipeline, tiny output

# Option B – EMR Serverless, step-by-step
STEPS=1 EMR_APPLICATION_ID=... JOB_ROLE_ARN=... bash aws/run_pipeline.sh
STEPS=1,2 MAX_WAT_PER_HOST=100 ... bash aws/run_pipeline.sh
STEPS=1,2,3 ... bash aws/run_pipeline.sh

# Sync output locally for notebook analysis
aws s3 sync s3://cc-pyspark/pipeline/cc-main-2024-aug-sep-oct/warehouse/weighted_links/ \
            cc/spark-warehouse/weighted_links/
```

Key env vars for `run_pipeline.sh` / `run_local.sh`:
- `WEBGRAPH_CRAWL` — host graph crawl (default: `cc-main-2024-aug-sep-oct`)
- `DATA_CRAWL` — CC-MAIN crawl ID for the columnar index (default: `CC-MAIN-2024-39`)
- `MAX_WAT_PER_HOST` — cap WAT records per host in step 2 (unset = no cap)
- `STEPS` — comma-separated list of steps to run, e.g. `1,2` or `1,2,3`

## Analysis Notebooks (cc/)

The notebooks read from `cc/spark-warehouse/` (locally synced pipeline output):

- `query_tables.ipynb` — General analysis: merges weighted links with `news_ratings_11k.csv` reliability labels, computes `weighted_average` (link-weighted unreliability score) per domain.
- `query_podcasts.ipynb` — Podcast-specific: filters sources to podcast hosting platforms (`podbean.com`, `libsyn.com`, `player.fm/series/`), produces `outlink_unreliability_score` per podcast domain.

**Key data files** (in `cc/data/`, gitignored):
- `news_ratings_11k.csv`: columns `domain`, `pc1` (reliability score 0–1, higher = more reliable)
- `link_dists.pkl`: cached `query_all_tables()` result to avoid re-running Spark

**Key analysis pattern**: `weighted_average = Σ(num_links × (1 - label)) / Σ(num_links)` — higher score means more links to unreliable sources.

## Dependencies

The pipeline imports from the official cc-pyspark (not the submodule):
```bash
# cc_pyspark.zip bundled into EMR job via --py-files:
zip cc_pyspark.zip cc-pyspark-official/cc-pyspark-official/sparkcc.py \
                   cc-pyspark-official/cc-pyspark-official/json_importer.py
```

Local dev / notebook environment:
```bash
conda activate cc_webgraph
pip install -r cc-pyspark/requirements.txt   # boto3, warcio, idna, pyspark, etc.
```

Initialize submodule:
```bash
git submodule update --init
```
