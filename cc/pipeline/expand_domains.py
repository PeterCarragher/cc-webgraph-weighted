"""
Step 1: Find all domains that link TO a set of target domains (backlinkers)
using the CC host-level webgraph.

CC webgraph format (host/vertices/ and host/edges/ under the crawl prefix):

  Vertex files (multi-part .txt.gz, tab-separated):
    <id>\t<host_reversed>
    e.g.:  12345\tcom.nytimes

  Edge files (multi-part .txt.gz, tab-separated):
    <src_id>\t<tgt_id>
    An edge src→tgt means the src host links TO the tgt host.

Backlinkers of target T = all src such that (src, T) is in the edge set.

Output: Parquet with single column 'host_reversed' (SURT format)
  containing the target domains themselves plus all their backlinkers.

Crawl naming convention: cc-main-YYYY-mon-mon-mon  (e.g. cc-main-2024-aug-sep-oct)
Webgraph S3 prefix:      s3://commoncrawl/projects/hyperlinkgraph/<crawl>/host/

Usage:
  spark-submit expand_domains.py \\
    --target_domains  s3://cc-pyspark/domains.csv \\
    --webgraph_prefix s3://commoncrawl/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/host \\
    --output          s3://cc-pyspark/pipeline/expanded_domains/
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target_domains', required=True,
                        help='Path to CSV with target domains (column: "domain", e.g. nytimes.com)')
    parser.add_argument('--webgraph_prefix', required=True,
                        help='S3 prefix for the CC host graph, e.g. '
                             's3://commoncrawl/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/host')
    parser.add_argument('--output', required=True,
                        help='Output path (Parquet) for expanded host set')
    return parser.parse_args()


@F.udf(StringType())
def to_surt(raw):
    """Extract host from a URL or bare domain and return in SURT format.

    'https://www.nytimes.com/section' -> 'com.nytimes'
    'mirror.co.uk/'                   -> 'uk.co.mirror'
    'ksat.com'                        -> 'com.ksat'
    """
    if not raw:
        return None
    s = raw.strip().lower()
    # Strip scheme
    if '://' in s:
        s = s.split('://', 1)[1]
    # Take host only (before first / ? #)
    for sep in ('/', '?', '#'):
        if sep in s:
            s = s.split(sep)[0]
    host = s.strip()
    if not host or '.' not in host:
        return None
    # Strip leading www.
    if host.startswith('www.'):
        host = host[4:]
    return '.'.join(reversed(host.split('.')))


def main():
    args = parse_args()
    spark = SparkSession.builder.appName('ExpandDomains').getOrCreate()

    vertices_path = args.webgraph_prefix.rstrip('/') + '/vertices'
    edges_path    = args.webgraph_prefix.rstrip('/') + '/edges'

    # ── Target domains ────────────────────────────────────────────────────────
    # Accepts CSV with or without a header; URL paths and trailing slashes are
    # stripped; www. is removed. Reads first column regardless of name.
    raw_csv = spark.read.csv(args.target_domains, header=False)
    first_col = raw_csv.columns[0]
    target_surt = (
        raw_csv
        .select(to_surt(F.col(first_col)).alias('host_reversed'))
        .dropna()
        .distinct()
    )

    # ── Webgraph vertices: <id>\t<host_reversed> ──────────────────────────────
    vertex_schema = StructType([
        StructField('id', LongType(), False),
        StructField('host_reversed', StringType(), True),
    ])
    vertices = spark.read.csv(vertices_path, sep='\t', schema=vertex_schema)

    # ── Webgraph edges: <src_id>\t<tgt_id> ────────────────────────────────────
    edge_schema = StructType([
        StructField('src', LongType(), False),
        StructField('tgt', LongType(), False),
    ])
    edges = spark.read.csv(edges_path, sep='\t', schema=edge_schema)

    # Map target domain names -> vertex IDs (small set; persist for reuse)
    target_ids = (
        target_surt
        .join(vertices, on='host_reversed', how='inner')
        .select(F.col('id'))
        .distinct()
        .persist()
    )

    target_count = target_ids.count()
    print(f'Found {target_count} of the target domains in the webgraph vertex list.')

    # ── Backlinkers: all src where (src -> tgt) and tgt is a target ───────────
    # Broadcast target_ids since it's tiny (at most as many rows as target domains)
    backlinker_ids = (
        edges
        .join(F.broadcast(target_ids.withColumnRenamed('id', 'tgt')), on='tgt', how='inner')
        .select(F.col('src').alias('id'))
        .distinct()
    )

    # Union target IDs + backlinker IDs, then resolve back to host names
    all_ids = target_ids.union(backlinker_ids).distinct()

    expanded = (
        all_ids
        .join(vertices, on='id', how='inner')
        .select('host_reversed')
        .distinct()
    )

    expanded.write.mode('overwrite').parquet(args.output)

    expanded_count = expanded.count()
    print(f'Expanded host set: {expanded_count} hosts '
          f'({expanded_count - target_count} backlinkers + {target_count} targets)')
    spark.stop()


if __name__ == '__main__':
    main()
