# OpenAlex AI Research: PySpark Data Processing

Week 13 Mini Assignment. The pipeline follows the class demonstration's progression from a lazy DataFrame to an action, a physical plan, aggregations, a broadcast join, SQL queries, caching, and saved Delta tables. It also includes the optional MLlib clustering task.

## Dataset and research question

The source is the [OpenAlex public Parquet snapshot](https://help.openalex.org/access/snapshot/), dated **2026-10-14**. Works contain publication years, topics, authorships, institutions, countries, and citation counts. The [Works manifest](https://openalex.s3.amazonaws.com/data/parquet/works/manifest.json) reported 1,898 files and 744.0 GB. The [Topics manifest](https://openalex.s3.amazonaws.com/data/parquet/topics/manifest.json) reported six files totaling 5.6 MB.

The question is: **How are publication years, research topics, affiliated countries and educational institutions, and international collaboration represented among AI works in a reproducible OpenAlex file sample?** These are sample findings. The selected files cannot establish global publication totals or growth rates.

Eight Works files were sampled uniformly without replacement from the full manifest using Python `random.Random(13)`, then sorted by URL. Three of those sorted files were sampled uniformly with `random.Random(1301)` and sorted by URL for reading. The three exact paths total **2,021,062,974 compressed bytes (2.02 GB)** in the 2026-10-14 Works manifest, exceeding the assignment's 1 GB requirement:

```text
s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0008.parquet
s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0011.parquet
s3://openalex/data/parquet/works/updated_date=2026-10-14/part_0084.parquet
```

OpenAlex partitions these files by the date a record was last updated, **not** its publication date. All three realized sample files have October 2026 update dates. Uniform file sampling gives each record an equal probability of selection at the file level, but three clusters leave substantial sampling uncertainty. The analysis is limited to publications from 2018 through 2025 whose primary topic belongs to the [Artificial Intelligence subfield (`1702`)](https://openalex.org/subfields/1702) and whose `is_xpac` value is false. API records were used to inspect the schema; the snapshot itself determines the reported results.

## Run the pipeline

Import or open [openalex_ai_pipeline.py](openalex_ai_pipeline.py) as a Databricks Python notebook and attach Serverless compute. Run the cells in order. The notebook reads the selected S3 Parquet files, writes a filtered AI subset as Delta, joins the small Topics lookup with a broadcast join, aggregates publication years, topics, countries, institutions, and international collaboration, runs two SQL queries, compares repeated counts with caching, runs KMeans clustering, and writes compact managed Delta tables under `workspace.default`.

The outputs are:

```text
workspace.default.openalex_ai_works
workspace.default.openalex_ai_annual
workspace.default.openalex_ai_topics
workspace.default.openalex_ai_countries
workspace.default.openalex_ai_institutions
workspace.default.openalex_ai_collaboration
workspace.default.openalex_ai_ml_clusters
```

The pipeline counts a paper once per country or educational institution, even when multiple coauthors share an affiliation. A multinational paper appears once for each represented country. The international collaboration share uses papers with at least one known affiliated country as its denominator. Citation averages are descriptive because newer publications have had less time to accumulate citations.

## Actions and transformations

The three `filter()` calls, `select()`, and `withColumn()` construct `ai_works` lazily. Printing the DataFrame and calling `explain()` show its schema and planned operators without executing the full record scan. The following `count()` is an action: it starts a distributed Spark job and returns the number of matching works. The later `show()` and Delta writes are actions too. This sequence makes the execution boundary visible in the notebook.

## Performance analysis

The initial filtered action returned **4,845 AI works** in **27.0 seconds**. The Photon Parquet scan covered the three exact S3 paths. Its physical plan listed the publication-year and `is_xpac` predicates under `DataFilters` and the primary AI subfield predicate under `DictionaryFilters`. It listed `PartitionFilters: []` because the read supplied exact files instead of a partition directory. The scan selected only columns needed by the filtered DataFrame. Query Details for this action showed **17/17 tasks completed**, **26.8 seconds** duration, **4,845 rows read**, and **6.72 MB bytes read** as reported by that view. Those metrics describe this query; they are not a measurement of all bytes in the 2.02 GB compressed input files.

The annual aggregation plan used `PhotonGroupingAgg` for partial aggregation before a hash exchange. Its Delta scan read only `publication_year`, `cited_by_count`, and `period`, confirming column pruning for that query. The Topics join used `PhotonBroadcastHashJoin` with the six small Topics files on the broadcast side, avoiding a shuffle of the Works side. For the initial count, Query Profile attributed **100% of aggregated operator time to the Scan** (2.91 minutes summed across parallel tasks), while its Shuffle operator took 9 ms; this does not mean the query's wall time was 2.91 minutes. The pipeline filters before exploding authorships and materializes the 4,845-row subset once. It reads three separate physical Parquet files and deliberately avoids `repartition()` before filtering. The saved subset and eight-row annual result are too small to justify publication-year partitioned output, which would produce tiny files; Spark performs a hash exchange only where grouping or deduplication requires one.

## Optional cache experiment

The final cache cell repeats the same filter on the original three Works Parquet files. Each `count()` returned **4,845** rows. The first uncached action took **14.17 seconds**; the action immediately after `.cache()` took **0.53 seconds**; a further cached action took **0.43 seconds**. The cell calls `.unpersist()` after the comparison. These are single-run, order-dependent timings. The much faster later actions demonstrate reuse in this session, but the measurement does not isolate DataFrame caching from warmed storage or other Databricks caching, so it is not a controlled estimate of the benefit of `.cache()` alone. The earlier comparison on the already compact Delta subset is less informative because all three counts took less than one second.

## Optional MLlib clustering

The notebook uses `pyspark.ml` `VectorAssembler` and `KMeans` with **k = 3**, **seed = 13**, and at most 20 iterations. Features are scaled publication year `(year - 2018) / 7` and `log1p(cited_by_count) / 7`; negative or missing citation counts are treated as zero for feature construction. The clustering uses all **4,845** filtered sample works and saves a three-row summary in `workspace.default.openalex_ai_ml_clusters`.

| Cluster | Sample works | Mean publication year | Mean citations |
| --- | ---: | ---: | ---: |
| 0 | 1,377 | 2020.0 | 4.4 |
| 1 | 2,521 | 2024.3 | 1.1 |
| 2 | 947 | 2022.1 | 38.9 |

These clusters describe publication time and citation counts in the selected files. Citation accumulation depends on article age, and KMeans labels do not identify research fields or explain why citations differ.

## Findings

The completed pipeline wrote seven managed Delta tables: **4,845** filtered works, **8** annual rows, **71** topic rows, **122** country rows, **2,365** educational-institution rows, **8** collaboration rows, and **3** ML cluster rows. The sample's annual counts rose from **279 in 2018** to **1,331 in 2025**. This describes the selected update-date file sample, not global OpenAlex growth. The most frequent Topics lookup label was **Sentiment Analysis and Opinion Mining** (**362** sample works). The largest country counts were **China 814**, **Indonesia 562**, and **United States 555**; a multinational work contributes to each of its countries. The highest educational-institution count was **Tsinghua University 46**. Among papers with a known affiliated country, the international-collaboration share ranged from **17.7% in 2024** to **22.3% in 2022**. These figures cannot support population rankings or a causal claim about ChatGPT because the three selected files are update-date clusters and the AI filter uses only the primary subfield.

## Screenshots

These screenshots were captured from the completed Databricks notebook. They are cropped to the English notebook and Query Details content.

- [Physical execution plan and pushed filters](screenshots/physical_plan.png)
- [Successful execution and six saved output tables](screenshots/pipeline_success.png)
- [Query Details for the initial filtered count](screenshots/query_details.png)
- [Query Profile showing the scan bottleneck and small count shuffle](screenshots/query_profile.png)
- [Successful MLlib KMeans execution and cluster summary](screenshots/ml_clusters.png)
- [Raw Parquet cache comparison and successful execution](screenshots/cache_comparison.png)

## Sources

- [OpenAlex snapshot format and partitioning](https://help.openalex.org/access/snapshot/)
- [OpenAlex Works attributes](https://help.openalex.org/data/works/attributes/)
- [OpenAlex Topics and hierarchy](https://help.openalex.org/data/topics/)
