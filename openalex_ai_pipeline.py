# Databricks notebook source
# Week 13 Mini Assignment: PySpark Data Processing
# Dataset: OpenAlex public Parquet snapshot, 2026-10-14.
# Run this notebook on Databricks Serverless from top to bottom.

# COMMAND ----------
from pyspark.sql import functions as F
from time import perf_counter
import requests

WORKS_PATHS = [
    "s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0008.parquet",
    "s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0011.parquet",
    "s3://openalex/data/parquet/works/updated_date=2026-10-14/part_0084.parquet",
]
AI_SUBFIELD = "https://openalex.org/subfields/1702"
OUTPUT_SCHEMA = "workspace.default"

raw_works = spark.read.parquet(*WORKS_PATHS)
print("Raw paths:", len(WORKS_PATHS))
print("Top-level columns:", len(raw_works.columns))

# COMMAND ----------
# Transformations build a plan. They do not process the full input yet.
ai_works = (
    raw_works
    .filter(F.col("publication_year").between(2018, 2025))
    .filter(F.col("is_xpac") == F.lit(False))
    .filter(F.col("primary_topic.subfield.id") == AI_SUBFIELD)
    .select(
        "id", "publication_year", "cited_by_count", "authorships",
        F.col("primary_topic.id").alias("topic_id"),
    )
    .withColumn(
        "period",
        F.when(F.col("publication_year") >= 2023, "2023-2025")
         .otherwise("2018-2022"),
    )
)
print("Lazy DataFrame:", ai_works)
ai_works.explain()

# count() is an action: it triggers Spark execution.
started = perf_counter()
print("AI works in the selected files:", ai_works.count())
print("Count seconds:", round(perf_counter() - started, 1))

# COMMAND ----------
# Preserve the small AI subset as a Delta table before repeated analysis.
ai_works.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.openalex_ai_works")
ai_works = spark.table(f"{OUTPUT_SCHEMA}.openalex_ai_works")
print("Saved filtered AI works:", ai_works.count())

# COMMAND ----------
# A grouped aggregation requires data exchange between workers.
annual_ai = (
    ai_works.groupBy("publication_year", "period")
    .agg(
        F.count("*").alias("sample_works"),
        F.round(F.avg("cited_by_count"), 1).alias("avg_citations"),
    )
    .orderBy("publication_year")
)
annual_ai.explain()
annual_ai.show(20, truncate=False)

# COMMAND ----------
# Topics is a small lookup dataset (5.6 MB in the 2026-10-14 manifest).
# Read its exact Parquet files from the official manifest and broadcast the join.
manifest_url = "https://openalex.s3.amazonaws.com/data/parquet/topics/manifest.json"
response = requests.get(manifest_url, timeout=120)
response.raise_for_status()
topics_manifest = response.json()
topic_paths = [entry["url"] for entry in topics_manifest["files"]]
topics_lookup = (
    spark.read.parquet(*topic_paths)
    .select(F.col("id").alias("topic_id"),
            F.col("display_name").alias("topic_name"))
)
ai_topics = ai_works.join(F.broadcast(topics_lookup), "topic_id", "left")
ai_topics.explain()

top_topics = (
    ai_topics.groupBy("topic_id", "topic_name")
    .agg(F.count("*").alias("sample_works"))
    .orderBy(F.desc("sample_works"))
)
top_topics.show(15, truncate=False)

# COMMAND ----------
# Count each paper once per country, even if several coauthors share that country.
country_works = (
    ai_works.select("id", "publication_year",
                    F.explode_outer("authorships").alias("authorship"))
    .select("id", "publication_year",
            F.explode_outer("authorship.countries").alias("country_code"))
    .filter(F.col("country_code").isNotNull())
    .dropDuplicates(["id", "country_code"])
)

top_countries = (
    country_works.groupBy("country_code")
    .agg(F.count("*").alias("sample_works"))
    .orderBy(F.desc("sample_works"))
)
top_countries.show(15, truncate=False)

# COMMAND ----------
# An educational institution is counted once per paper.
institution_works = (
    ai_works.select("id", F.explode_outer("authorships").alias("authorship"))
    .select("id", F.explode_outer("authorship.institutions").alias("institution"))
    .select("id", F.col("institution.id").alias("institution_id"),
            F.col("institution.display_name").alias("institution_name"),
            F.col("institution.type").alias("institution_type"))
    .filter((F.col("institution_id").isNotNull()) &
            (F.col("institution_type") == "education"))
    .dropDuplicates(["id", "institution_id"])
)
top_institutions = (
    institution_works.groupBy("institution_id", "institution_name")
    .agg(F.count("*").alias("sample_works"))
    .orderBy(F.desc("sample_works"))
)
top_institutions.show(15, truncate=False)

# International collaboration uses papers with at least one known country.
collaboration_by_year = (
    country_works.groupBy("id", "publication_year")
    .agg(F.count("*").alias("country_count"))
    .withColumn("international", F.col("country_count") >= 2)
    .groupBy("publication_year")
    .agg(
        F.count("*").alias("works_with_known_country"),
        F.sum(F.col("international").cast("int")).alias("international_works"),
        F.round(F.avg(F.col("international").cast("int")) * 100, 1)
         .alias("international_share_pct"),
    )
    .orderBy("publication_year")
)
collaboration_by_year.show(20, truncate=False)

# COMMAND ----------
# SQL query 1: annual AI publication counts in the selected file sample.
ai_works.createOrReplaceTempView("ai_works_sample")
spark.sql("""
    SELECT publication_year, COUNT(*) AS sample_works
    FROM ai_works_sample
    GROUP BY publication_year
    ORDER BY publication_year
""").show(20, truncate=False)

# SQL query 2: countries represented in AI paper authorships.
country_works.createOrReplaceTempView("ai_country_works_sample")
spark.sql("""
    SELECT country_code, COUNT(*) AS sample_works
    FROM ai_country_works_sample
    GROUP BY country_code
    ORDER BY sample_works DESC, country_code
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------
# Optional cache demonstration. Materialization is timed separately from reuse.
started = perf_counter()
uncached_count = ai_works.count()
uncached_seconds = perf_counter() - started

ai_works.cache()
started = perf_counter()
materialized_count = ai_works.count()
materialization_seconds = perf_counter() - started

started = perf_counter()
cached_count = ai_works.count()
cached_seconds = perf_counter() - started

print("Uncached count:", uncached_count, "seconds:", round(uncached_seconds, 2))
print("Cache materialization count:", materialized_count,
      "seconds:", round(materialization_seconds, 2))
print("Cached repeat count:", cached_count, "seconds:", round(cached_seconds, 2))
# Keep the cache available to the dependent output writes below.

# COMMAND ----------
# Persist compact results as managed Delta tables, as in the class demonstration.
annual_ai.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.openalex_ai_annual")
top_topics.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.openalex_ai_topics")
top_countries.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.openalex_ai_countries")
top_institutions.write.mode("overwrite").saveAsTable(
    f"{OUTPUT_SCHEMA}.openalex_ai_institutions")
collaboration_by_year.write.mode("overwrite").saveAsTable(
    f"{OUTPUT_SCHEMA}.openalex_ai_collaboration")

print("Saved output tables:")
for table_name in ("openalex_ai_works", "openalex_ai_annual",
                   "openalex_ai_topics", "openalex_ai_countries",
                   "openalex_ai_institutions", "openalex_ai_collaboration"):
    table = f"{OUTPUT_SCHEMA}.{table_name}"
    print(table, spark.table(table).count(), "rows")

# Release cached data after the dependent output writes have completed.
ai_works.unpersist(blocking=True)

# COMMAND ----------
# Optional MLlib task: cluster sample AI works by publication time and citations.
# Fixed scaling keeps both numeric features on a roughly comparable range.
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

ml_works = (
    spark.table(f"{OUTPUT_SCHEMA}.openalex_ai_works")
    .select("id", "publication_year", "cited_by_count")
    .filter(F.col("publication_year").between(2018, 2025))
    .withColumn("year_scaled", (F.col("publication_year") - F.lit(2018)) / F.lit(7.0))
    .withColumn(
        "log_citations_scaled",
        F.log1p(F.greatest(F.coalesce(F.col("cited_by_count"), F.lit(0)), F.lit(0)))
        / F.lit(7.0),
    )
)
assembler = VectorAssembler(
    inputCols=["year_scaled", "log_citations_scaled"], outputCol="features"
)
ml_input = assembler.transform(ml_works)
kmeans = KMeans(k=3, seed=13, maxIter=20, featuresCol="features",
                predictionCol="cluster")
kmeans_model = kmeans.fit(ml_input)
ml_predictions = kmeans_model.transform(ml_input)
cluster_summary = (
    ml_predictions.groupBy("cluster")
    .agg(
        F.count("*").alias("sample_works"),
        F.round(F.avg("publication_year"), 1).alias("mean_publication_year"),
        F.round(F.avg("cited_by_count"), 1).alias("mean_citations"),
    )
    .orderBy("cluster")
)
cluster_summary.write.mode("overwrite").saveAsTable(
    f"{OUTPUT_SCHEMA}.openalex_ai_ml_clusters"
)
print("MLlib algorithm: KMeans; k=3; seed=13")
print("Input works:", ml_input.count())
print("Cluster summary:")
cluster_summary.show(truncate=False)

# COMMAND ----------
# Optional cache bonus: repeat the expensive raw Parquet filter.
raw_cache_probe = (
    spark.read.parquet(*WORKS_PATHS)
    .filter(F.col("publication_year").between(2018, 2025))
    .filter(F.col("is_xpac") == F.lit(False))
    .filter(F.col("primary_topic.subfield.id") == AI_SUBFIELD)
    .select("id", "publication_year", "cited_by_count")
)

started = perf_counter()
raw_uncached_count = raw_cache_probe.count()
raw_uncached_seconds = perf_counter() - started

raw_cache_probe.cache()
started = perf_counter()
raw_materialized_count = raw_cache_probe.count()
raw_materialization_seconds = perf_counter() - started

started = perf_counter()
raw_cached_count = raw_cache_probe.count()
raw_cached_seconds = perf_counter() - started

assert raw_uncached_count == raw_materialized_count == raw_cached_count
print("Raw Parquet uncached count:", raw_uncached_count,
      "seconds:", round(raw_uncached_seconds, 2))
print("Raw Parquet cache materialization count:", raw_materialized_count,
      "seconds:", round(raw_materialization_seconds, 2))
print("Raw Parquet cached repeat count:", raw_cached_count,
      "seconds:", round(raw_cached_seconds, 2))
raw_cache_probe.unpersist(blocking=True)
