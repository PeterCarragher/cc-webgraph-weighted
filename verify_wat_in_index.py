#!/usr/bin/env python3
"""
Quick verification: Does the CC columnar index contain 'subset=wat' records?

Usage:
  spark-submit verify_wat_in_index.py
"""

from pyspark.sql import SparkSession

# Pick a recent crawl to check
CRAWL = "CC-MAIN-2024-39"
INDEX_PATH = "s3://commoncrawl/cc-index/table/cc-main/warc/"

spark = SparkSession.builder \
    .appName("verify-wat") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

print(f"Checking columnar index for crawl {CRAWL}...")
print(f"Index path: {INDEX_PATH}\n")

# Read one crawl's data
df = spark.read.parquet(INDEX_PATH).filter(f"crawl = '{CRAWL}'")

# Count records by subset
print("Record counts by subset:")
subset_counts = df.groupBy('subset').count().orderBy('subset').collect()

for row in subset_counts:
    print(f"  {row['subset']:10s}: {row['count']:,} records")

# Check if 'wat' exists
subsets = [row['subset'] for row in subset_counts]
print(f"\nAvailable subsets: {subsets}")
print(f"WAT in index: {'YES' if 'wat' in subsets else 'NO'}")

spark.stop()
