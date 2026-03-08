"""
Step 3: Extract weighted host-level links from WARC records.

Reads WARC record coordinates produced by get_wat_index.py, fetches the
records from S3, parses HTML for outgoing links, and counts links between
host pairs.

Unlike the standard ExtractHostLinksJob (which deduplicates to a binary
edge per page), this job counts every outgoing link, yielding:
  (source_host_surt, target_host_surt, count_per_page)

These are then aggregated to:
  (s, t, total_count)   -- total links from host s to host t across all pages

Output schema: s STRING, t STRING, count LONG
  Both s and t are in SURT (reversed) format, e.g. 'com.nytimes'

Usage (EMR Serverless or local):
  spark-submit \\
    --py-files cc_pyspark.zip \\
    weighted_extract_links.py \\
    s3://bucket/output/warc_index/ \\
    weighted_links \\
    --input_table_format parquet \\
    --input_base_url s3://commoncrawl/ \\
    --num_input_partitions 2000 \\
    --num_output_partitions 200
"""

import re
import idna

from collections import Counter
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Import from official cc-pyspark (must be on PYTHONPATH or in --py-files zip)
from sparkcc import CCIndexWarcSparkJob


def _domain_to_surt(raw):
    """Convert a domain or URL to SURT format. Returns None if invalid."""
    if not raw:
        return None
    s = raw.strip().lower()
    if '://' in s:
        s = s.split('://', 1)[1]
    for sep in ('/', '?', '#'):
        if sep in s:
            s = s.split(sep)[0]
    host = s.strip()
    if not host or '.' not in host:
        return None
    if host.startswith('www.'):
        host = host[4:]
    return '.'.join(reversed(host.split('.')))


class _LinkExtractor(HTMLParser):
    """Minimal HTML parser that collects <a href> and <base href> values."""

    def __init__(self):
        super().__init__()
        self.hrefs = []
        self.base = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == 'base' and self.base is None:
            self.base = attrs_dict.get('href')
        elif tag == 'a':
            href = attrs_dict.get('href')
            if href:
                self.hrefs.append(href)

    def error(self, message):
        pass  # ignore parse errors


class WeightedExtractHostLinksJob(CCIndexWarcSparkJob):
    """Extract weighted host-level links from WARC records accessed via CC index.

    Parses HTML from WARC response records and counts all outgoing <a href>
    links per (source_host, target_host) pair rather than deduplicating to a
    binary edge, enabling downstream weighted webgraph analysis.
    """

    name = 'WeightedExtractHostLinks'

    # Broadcast set of target SURT hosts (populated in run_job before workers run)
    target_hosts_bc = None

    output_schema = StructType([
        StructField('s', StringType(), True),
        StructField('t', StringType(), True),
        StructField('count', LongType(), True),
    ])

    # Parse HTTP headers so record.http_headers is available in process_record
    warc_parse_http_header = True

    # Accumulators
    records_warc = None
    records_non_html = None
    records_failed = None
    link_count = None

    # ── Regex patterns ────────────────────────────────────────────────────────

    host_ip_pattern = re.compile(
        r'^(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[0-9a-f]{0,4}:[0-9a-f:]+)\Z')

    host_part_pattern = re.compile(
        r'^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?\Z', re.IGNORECASE | re.ASCII)

    url_parse_host_pattern = re.compile(
        r'^https?://([a-z0-9_.-]{2,253})(?:[/?#]|\Z)', re.IGNORECASE | re.ASCII)

    # ── Host extraction ───────────────────────────────────────────────────────

    @staticmethod
    def get_surt_host(url):
        """Extract and reverse hostname from URL. Returns None if invalid."""
        m = WeightedExtractHostLinksJob.url_parse_host_pattern.match(url)
        if m:
            host = m.group(1)
        else:
            try:
                host = urlparse(url).hostname
            except Exception:
                return None
            if not host:
                return None
        host = host.strip().lower()
        if len(host) < 1 or len(host) > 253:
            return None
        if WeightedExtractHostLinksJob.host_ip_pattern.match(host):
            return None
        parts = host.split('.')
        if parts[-1] == '':
            parts = parts[:-1]
        if len(parts) <= 1:
            return None
        for i, part in enumerate(parts):
            if len(part) > 63:
                return None
            if not WeightedExtractHostLinksJob.host_part_pattern.match(part):
                try:
                    idn = idna.encode(part).decode('ascii')
                except Exception:
                    return None
                if WeightedExtractHostLinksJob.host_part_pattern.match(idn):
                    parts[i] = idn
                else:
                    return None
        parts.reverse()
        return '.'.join(parts)

    # ── HTML link extraction ──────────────────────────────────────────────────

    def _get_link_counts(self, url, html_bytes, src_host):
        """Parse HTML bytes and return Counter of {thost: link_count}."""
        target_filter = self.target_hosts_bc.value if self.target_hosts_bc else None
        counts = Counter()
        try:
            html_str = html_bytes.decode('utf-8', errors='replace')
            extractor = _LinkExtractor()
            extractor.feed(html_str)

            base_url = url
            if extractor.base:
                try:
                    base_url = urljoin(url, extractor.base)
                except Exception:
                    pass

            for href in extractor.hrefs:
                try:
                    abs_url = urljoin(base_url, href)
                    thost = self.get_surt_host(abs_url)
                except Exception:
                    continue
                if thost and thost != src_host:
                    if target_filter is None or thost in target_filter:
                        counts[thost] += 1
        except Exception:
            pass
        return counts

    # ── WARC record processing ────────────────────────────────────────────────

    def process_record(self, record):
        """Process a single WARC response record; yield (url, t_host, count)."""
        if record.rec_type != 'response':
            return

        url = record.rec_headers.get_header('WARC-Target-URI')
        if not url:
            return

        # Check HTTP 200 and HTML content-type
        if record.http_headers:
            if record.http_headers.get_statuscode() != '200':
                return
            ct = record.http_headers.get_header('Content-Type', '')
            if 'text/html' not in ct.lower():
                self.records_non_html.add(1)
                return

        src_host = self.get_surt_host(url)
        if not src_host:
            return

        try:
            html_bytes = self.get_payload_stream(record).read()
        except Exception as e:
            self.get_logger().error('Failed to read WARC payload for {}: {}'.format(url, e))
            self.records_failed.add(1)
            return

        self.records_warc.add(1)
        counts = self._get_link_counts(url, html_bytes, src_host)
        for thost, count in counts.items():
            self.link_count.add(count)
            yield url, thost, count

    # ── Accumulators ─────────────────────────────────────────────────────────

    def init_accumulators(self, session):
        super().init_accumulators(session)
        sc = session.sparkContext
        self.records_warc = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)
        self.records_failed = sc.accumulator(0)
        self.link_count = sc.accumulator(0)

    def log_accumulators(self, session):
        super().log_accumulators(session)
        self.log_accumulator(session, self.records_warc,
                             'WARC response records processed = {}')
        self.log_accumulator(session, self.records_non_html,
                             'non-HTML records skipped = {}')
        self.log_accumulator(session, self.records_failed,
                             'records failed = {}')
        self.log_accumulator(session, self.link_count,
                             'total links counted = {}')

    # ── Job orchestration ─────────────────────────────────────────────────────

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--target_domains', required=False, default=None,
            help='CSV of target domains (same file as step 1). When provided, only '
                 'links whose target host is in this set are counted. If omitted, '
                 'all external links are counted.')

    def run_job(self, session):
        # Build broadcast set of target SURT hosts (if requested)
        if getattr(self.args, 'target_domains', None):
            raw = session.read.csv(self.args.target_domains, header=False)
            first_col = raw.columns[0]
            target_surts = set(
                row[0] for row in raw.select(first_col).collect()
                if row[0] and _domain_to_surt(row[0])
            )
            # Store as SURT
            target_surts = {_domain_to_surt(h) for h in target_surts if _domain_to_surt(h)}
            self.target_hosts_bc = session.sparkContext.broadcast(target_surts)

        # Load WARC record coordinates (output of get_wat_index.py)
        sqldf = self.load_dataframe(session, self.args.num_input_partitions)
        warc_recs = sqldf.select(
            'url', 'warc_filename', 'warc_record_offset', 'warc_record_length'
        ).rdd

        # Fetch WARC records from S3 and extract (source_url, target_host, count) tuples
        raw = warc_recs.mapPartitions(self.fetch_process_warc_records)

        # Resolve source URL -> source host SURT, then sum counts per (s, t) pair
        (
            session.createDataFrame(raw, schema=self.output_schema)
            # s column currently holds the source URL; convert to SURT host
            .withColumn('s', F.udf(self.get_surt_host, StringType())(F.col('s')))
            .dropna(subset=['s', 't'])
            .filter(F.col('s') != F.col('t'))  # drop self-links
            .groupBy('s', 't')
            .agg(F.sum('count').alias('count'))
            .coalesce(self.args.num_output_partitions)
            .write
            .format(self.args.output_format)
            .option('compression', self.args.output_compression)
            .options(**self.get_output_options())
            .saveAsTable(self.args.output)
        )

        self.log_accumulators(session)


if __name__ == '__main__':
    job = WeightedExtractHostLinksJob()
    job.run()
