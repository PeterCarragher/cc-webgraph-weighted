# cc-webgraph-weighted

Compute weighted domain-level link graphs from [Common Crawl](https://commoncrawl.org/) WARC data.

Links are classified into four buckets based on HTML context:
- `nav_count` — inside `<header>`, `<footer>`, `<nav>`, `<aside>`, or elements whose class/id suggests site-wide navigation
- `body_count` — inside `<article>`, `<main>`, `<p>`, or elements whose class/id suggests article body / prose
- `related_count` — inside elements whose class/id suggests related-article widgets or recommendation carousels
- `other_count` — everything else (sidebars, bylines, share buttons, uncontextualized links)

## Step 1 — Define target domains

Create a CSV of domains to compute link weights between (one per line, no header required):

```
bbc.co.uk
theguardian.com
nytimes.com
...
```

### Optional: expand to backlinkers

To include all domains that link *to* your seeds (so links between backlinkers are also
captured), run the domain expansion locally before the Spark job:

```bash
pip install pyccwebgraph tldextract  # requires Java 17+

python expand_domains_local.py \
    --seeds target_domains.csv \
    --output expanded_domains.csv \
    --version cc-main-2024-aug-sep-oct \
    --webgraph_dir /path/to/local/webgraph   # omit to auto-download (~30 GB)
```

Use `expanded_domains.csv` as `--target_domains` in Step 3.

## Step 2 — Upload code to S3

Clone cc-pyspark, build the library zip, and upload everything EMR needs:

```bash
git clone https://github.com/commoncrawl/cc-pyspark
cd cc-pyspark
pip install -r requirements.txt

# Build cc_pyspark.zip (the Spark --py-files dependency bundle)
zip cc_pyspark.zip sparkcc.py sparkcc_fastwarc.py

S3_CODE=s3://my-bucket/count_domain_links/code
aws s3 cp count_domain_links.py       $S3_CODE/count_domain_links.py
aws s3 cp cc_pyspark.zip              $S3_CODE/cc_pyspark.zip
aws s3 cp /path/to/target_domains.csv s3://my-bucket/target_domains.csv
```

`count_domain_links.py` requires `tldextract` and `warcio`, which are not bundled with EMR.
Build a virtual environment archive on a Linux x86-64 host matching the EMR runtime:

```bash
python -m venv pyspark_venv
source pyspark_venv/bin/activate
pip install tldextract warcio
venv-pack -o pyspark_venv.tar.gz
aws s3 cp pyspark_venv.tar.gz s3://my-bucket/emr/pyspark_venv.tar.gz
```

## Step 3 — Run the Spark job on AWS EMR Serverless

```bash
EMR_APPLICATION_ID=<your-emr-serverless-application-id> \
JOB_ROLE_ARN=arn:aws:iam::<account-id>:role/<execution-role> \
VENV_S3=s3://my-bucket/emr/pyspark_venv.tar.gz \
JOB_NAME=count_domain_links \
bash run_ccpyspark_job_aws_emr.sh \
    s3://my-bucket/count_domain_links/code/count_domain_links.py \
    s3://my-bucket/count_domain_links/warehouse \
    s3://commoncrawl/cc-index/table/cc-main/warc/ \
    domain_links \
    --input_base_url s3://commoncrawl/ \
    --target_domains s3://my-bucket/target_domains.csv \
    --crawl CC-MAIN-2024-38 \
    --num_input_partitions 200 \
    --num_output_partitions 5
```

**Key arguments:**
- `s3://commoncrawl/cc-index/table/cc-main/warc/` — CC columnar index (public, requester-pays)
- `domain_links` — output table name; written to `<warehouse>/default/domain_links/`
- `--crawl` — CC crawl ID, e.g. `CC-MAIN-2024-38`; see [Common Crawl releases](https://commoncrawl.org/blog)
- `--num_input_partitions` — set to ~200 for ~1,000 domains; scale up for larger sets
- `--num_output_partitions` — number of output parquet files (5–20 is typically fine)

**Optional resource tuning** (env vars for `run_ccpyspark_job_aws_emr.sh`):
```bash
EXECUTOR_MEM=16g    # default
EXECUTOR_CORES=4    # default
DRIVER_MEM=8g       # default
```

### Costs / Logistics
The logistics and costs of running this spark job on AWS EMR for a target_domain list of ~1400 popular domains are so follows:
- ~3M WARC files parsed
- ~3 hours runtime with 64vCPU limit
- 204.477 vCPU-hours billed
- 891.162 memoryGB-hours billed
- 1022.383 storageGB-hours (not billed, WARC files are already on AWS)
- Total AWS bill: ~$5 

## Step 4 — Download results

```bash
aws s3 sync \
    s3://my-bucket/count_domain_links/warehouse/default/domain_links/ \
    results/domain_links/
```

Output schema: `s STRING, t STRING, nav_count LONG, body_count LONG, related_count LONG, other_count LONG`

## Step 5 — Analyse results

Open [news_links_analysis.ipynb](news_links_analysis.ipynb) and update the `glob` path to your
downloaded results:

```python
files = glob.glob('results/domain_links/part-*.parquet')
```

Install notebook dependencies if needed:

```bash
pip install pandas pyarrow matplotlib
```

## TODOs

- Add option to cc-pyspark job for running computing links from domain set A to domain set B. This is useful, as currently, if we include backlinkers, we also capture links between backlinkers. So, this mode would enable a weighted webgraph with weighted links: [(backlinkers + original domains) --> (original domains)].
- Tune --num_input_partitions and --num_output_partitions automatically (based on number of domains and the number of retrieved WARC files).