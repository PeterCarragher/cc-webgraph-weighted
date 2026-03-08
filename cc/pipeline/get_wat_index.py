"""
Step 2: Query the CC columnar index for WAT record coordinates.

Given the expanded host set from expand_domains.py, finds the WAT record
coordinates (filename, offset, length) for HTML pages from those hosts
in a specified CC crawl.  WAT files contain pre-parsed link metadata,
making them much more efficient to process than raw WARC HTML.

CC columnar index path:
  s3://commoncrawl/cc-index/table/cc-main/warc/
  (Parquet, partitioned by crawl and subset; requires AWS credentials)

Output columns (consumed by weighted_extract_links.py via --input_table_format):
  url, warc_filename, warc_record_offset, warc_record_length

Note: 'warc_filename' in the index is a relative path (no s3://commoncrawl/ prefix).
Pass --input_base_url s3://commoncrawl/ to weighted_extract_links.py.

Usage:
  spark-submit get_wat_index.py \\
    --expanded_domains s3://bucket/output/expanded_domains/ \\
    --cc_index         s3://commoncrawl/cc-index/table/cc-main/warc/ \\
    --crawl            CC-MAIN-2024-10 \\
    --output           s3://bucket/output/wat_index/
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expanded_domains', required=True,
                        help='Path to expanded domains Parquet (from expand_domains.py)')
    parser.add_argument('--cc_index',
                        default='s3://commoncrawl/cc-index/table/cc-main/warc/',
                        help='Path to CC columnar index (Parquet)')
    parser.add_argument('--crawl', required=True,
                        help='CC crawl ID, e.g. CC-MAIN-2024-10')
    parser.add_argument('--output', required=True,
                        help='Output path (Parquet) for WAT record coordinates')
    parser.add_argument('--max_records_per_host', type=int, default=None,
                        help='Cap WAT records per host to control job cost (optional)')
    return parser.parse_args()


@F.udf(StringType())
def unreverse(host_reversed):
    """'com.nytimes' -> 'nytimes.com'"""
    if not host_reversed:
        return None
    return '.'.join(reversed(host_reversed.split('.')))


def main():
    args = parse_args()
    spark = SparkSession.builder.appName('GetWatIndex').getOrCreate()

    # Expanded host set: Parquet with column 'host_reversed' (e.g. 'com.nytimes')
    # Convert back to normal hostname form for matching the CC index url_host_name.
    # The webgraph strips leading 'www.' so 'com.nytimes' maps to 'nytimes.com',
    # which in turn matches url_host_name values like 'www.nytimes.com' after
    # we strip 'www.' from the index side.
    expanded_hosts = (
        spark.read.parquet(args.expanded_domains)
        .select(unreverse(F.col('host_reversed')).alias('host'))
        .distinct()
    )

    # CC columnar index, filtered to WARC response records for this crawl.
    # Note: 'subset=wat' was removed from the CC columnar index; 'subset=warc'
    # provides byte-range coordinates for raw WARC response records (HTML pages).
    index = (
        spark.read.parquet(args.cc_index)
        .filter(F.col('crawl') == args.crawl)
        .filter(F.col('subset') == 'warc')
        .filter(F.col('fetch_status') == 200)
        .filter(F.col('content_mime_detected') == 'text/html')
    )

    # Join on url_host_registered_domain (eTLD+1, e.g. 'mirror.co.uk') which
    # aligns with the webgraph's apex-domain granularity (www. stripped).
    matched = index.join(
        F.broadcast(expanded_hosts),
        index.url_host_registered_domain == expanded_hosts.host,
        how='inner'
    )

    if args.max_records_per_host is not None:
        w = Window.partitionBy('url_host_name').orderBy(F.rand())
        matched = (
            matched
            .withColumn('_rn', F.row_number().over(w))
            .filter(F.col('_rn') <= args.max_records_per_host)
        )

    result = matched.select('url', 'warc_filename', 'warc_record_offset', 'warc_record_length')

    result.write.mode('overwrite').parquet(args.output)
    spark.stop()


if __name__ == '__main__':
    main()
