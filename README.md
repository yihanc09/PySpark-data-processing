# Mapping AI Research with OpenAlex and PySpark

AI research is often described as a global race, but a publication count alone does not tell us who is contributing, which topics are active, or how researchers work across borders. I used OpenAlex to explore those questions through papers published from 2018 to 2025. My analysis looks at publication years, AI topics, author-affiliated countries and educational institutions, and papers with affiliations in more than one country. I also wanted to see what it takes to process this kind of bibliographic data efficiently in Spark.

The result is a PySpark pipeline that reads **2.02 GB of OpenAlex Parquet files**, identifies **4,845 AI papers**, joins topic labels, and writes publication, affiliation, and collaboration summaries to Delta tables in Databricks. I use the same dataset to inspect Spark's physical plans, compare repeated actions with caching, and try a small MLlib clustering model.

## 1. Data processing pipeline

### Dataset and loading

I used the [OpenAlex public Parquet snapshot](https://help.openalex.org/access/snapshot/) dated **October 14, 2026**. The Works data includes publication dates, citation counts, primary topics, authorships, and institutional affiliations. A separate Topics dataset provides readable topic names. The snapshot's [Works manifest](https://openalex.s3.amazonaws.com/data/parquet/works/manifest.json) listed 1,898 files totaling about 744 GB; the [Topics manifest](https://openalex.s3.amazonaws.com/data/parquet/topics/manifest.json) listed six files totaling about 5.6 MB.

Processing all Works files was unnecessary for this project, so I selected files with a fixed random seed. I first drew eight files from the manifest with `random.Random(13)`, then drew three of those with `random.Random(1301)`. Their combined compressed size in the manifest is **2,021,062,974 bytes**:

```text
s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0008.parquet
s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0011.parquet
s3://openalex/data/parquet/works/updated_date=2026-10-14/part_0084.parquet
```

OpenAlex organizes these files by **last update date**, not publication year. The publication-year analysis below uses each work's `publication_year` field; the dates in the file paths identify the source partitions.

For the analysis, an AI work is a paper published in **2018–2025** whose primary topic belongs to the OpenAlex [Artificial Intelligence subfield (`1702`)](https://openalex.org/subfields/1702). I also exclude records with `is_xpac = true`. This is a specific, repeatable definition of AI; it will miss papers that have AI as a secondary topic.

### Filters, transformations, join, and aggregations

The notebook first reads the three Works files directly from the public S3 snapshot. It applies three filters—publication year, `is_xpac`, and primary-topic subfield—before selecting the columns needed later. A `withColumn()` expression labels each paper as `2018–2022` or `2023–2025`. I save the filtered papers as a Delta table so the later summaries do not have to scan the large source files repeatedly.

I join those papers to the much smaller Topics dataset to attach topic names. The other summaries use `groupBy()` aggregations to count papers by year, topic, country, and educational institution. For country and institution counts, I deduplicate each paper–affiliation pair: two coauthors from the same country do not make one paper count twice for that country. A paper with affiliations in two countries counts once in each country's total. The international-collaboration rate is the share of papers with **at least two distinct affiliated countries**, among papers with at least one known country.

### Two Spark SQL queries

I create temporary views from the filtered papers and the deduplicated paper–country pairs. The first query checks annual paper counts; the second lists the countries most often represented in the selected works:

```sql
SELECT publication_year, COUNT(*) AS sample_works
FROM ai_works_sample
GROUP BY publication_year
ORDER BY publication_year;

SELECT country_code, COUNT(*) AS sample_works
FROM ai_country_works_sample
GROUP BY country_code
ORDER BY sample_works DESC, country_code
LIMIT 15;
```

### Written results

The completed run wrote these managed Delta tables under `workspace.default`:

| Table | Contents | Rows |
| --- | --- | ---: |
| `openalex_ai_works` | Filtered AI papers | 4,845 |
| `openalex_ai_annual` | Publication year and citation summaries | 8 |
| `openalex_ai_topics` | Topic counts after the lookup join | 71 |
| `openalex_ai_countries` | Counts by affiliated country | 122 |
| `openalex_ai_institutions` | Counts by educational institution | 2,365 |
| `openalex_ai_collaboration` | Cross-country collaboration by year | 8 |
| `openalex_ai_ml_clusters` | Optional MLlib cluster summary | 3 |

## Key findings from the data

The year-by-year table connects publication volume with citations and collaboration. “Known-country papers” is the denominator for the international share; papers without a country in their authorship metadata are excluded from that rate.

| Year | AI papers | Mean citations | Known-country papers | International papers | International share |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2018 | 279 | 18.9 | 198 | 37 | 18.7% |
| 2019 | 298 | 34.1 | 227 | 44 | 19.4% |
| 2020 | 374 | 16.1 | 284 | 59 | 20.8% |
| 2021 | 454 | 14.9 | 384 | 71 | 18.5% |
| 2022 | 564 | 10.3 | 470 | 105 | 22.3% |
| 2023 | 738 | 7.8 | 607 | 131 | 21.6% |
| 2024 | 807 | 5.1 | 677 | 120 | 17.7% |
| 2025 | 1,331 | 1.3 | 790 | 144 | 18.2% |

I see many more selected works in 2025 than in 2018: **1,331 versus 279**. The mean citation count is much lower for recent papers, which is expected because they have had less time to be cited; the relatively high **2019 mean of 34.1** also shows why a simple average can be influenced by a few highly cited works. The count of international papers grows from **37** to **144**, but the *share* stays roughly between **18% and 22%** in most years. In 2022 it reaches **22.3%**; in 2024 it is **17.7%**. Looking at the numerator and the denominator together gives a clearer picture than comparing collaboration counts alone.

The topic results show different kinds of AI work in the same sample. **Sentiment Analysis and Opinion Mining (362)** and **Topic Modeling (260)** lead the table, followed by **Privacy-Preserving Technologies in Data (203)**. Applied areas also appear: **AI in cancer detection (170)**, **Reinforcement Learning in Robotics (135)**, and **Natural Language Processing Techniques (134)**. These are OpenAlex's topic labels attached through the lookup join, not keyword counts from paper titles.

Affiliations add another perspective. The leading country counts are **China (814)**, **Indonesia (562)**, **United States (555)**, **India (250)**, and **United Kingdom (205)**. A paper can appear under more than one country, so these totals are not mutually exclusive. The leading educational institutions are **Tsinghua University (46)**, **University of Chinese Academy of Sciences (41)**, and **Telkom University (36)**; **Shanghai Jiao Tong University** and **MIT** each have **29**. Counting distinct paper–institution pairs prevents a university from gaining extra counts just because several coauthors share it. The selected update-date files make these useful descriptions of the processed data, while broader geographic comparisons would need more files.

## 2. Performance analysis and execution plans

The first `count()` on the filtered Works DataFrame returned **4,845** rows in **27.0 seconds**. Its physical plan shows a Photon Parquet scan over the three source paths. Spark placed the year and `is_xpac` tests in `DataFilters` and the AI subfield test in `DictionaryFilters`; the scan read only columns needed for this stage. The plan lists `PartitionFilters: []` because I supplied individual files rather than asking Spark to discover partitions from a directory. In Databricks Query Details, this action completed **17 of 17 tasks** in **26.8 seconds**. The view reported **4,845 rows** and **6.72 MB** read for this query; that query metric should not be confused with the 2.02 GB combined size of the input files.

The later plans show where distributed work becomes necessary. Spark uses `PhotonGroupingAgg` to aggregate locally before exchanging data for the annual totals. That query's Delta scan reads only `publication_year`, `cited_by_count`, and `period`, which is a useful example of column pruning. For the topic lookup, `PhotonBroadcastHashJoin` sends the small Topics side to workers instead of shuffling the larger Works side. I kept the filters ahead of the authorship explosions, avoided repartitioning the raw input, and saved the 4,845-row AI subset once for reuse. Partitioning the small output tables by publication year would create tiny partitions rather than help this workload.

The Query Profile makes the main bottleneck clear: the initial scan accounts for **100% of aggregated operator time** in that profile, while its shuffle operator accounts for **9 ms**. The displayed **2.91 minutes** for the scan is time summed across parallel tasks, not elapsed query time. Reading the source files dominates this first action; after filtering and materializing the much smaller Delta table, the aggregations and SQL queries work on a more manageable dataset. [The execution-plan screenshot](screenshots/physical_plan.png), [Query Details](screenshots/query_details.png), and [Query Profile](screenshots/query_profile.png) show the underlying evidence.

### Caching repeated actions

I compared repeated `count()` actions on the same raw Parquet filter. Each returned **4,845** papers. The first, uncached action took **14.17 seconds**; the action immediately after `.cache()` took **0.53 seconds**; another cached action took **0.43 seconds**. The code releases the cached DataFrame with `.unpersist()` afterward. [The cache screenshot](screenshots/cache_comparison.png) shows the code and output.

These are measurements from one ordered run, not a controlled speedup estimate. Later queries can also benefit from warmed storage or Databricks caching, so I cannot attribute the entire difference to Spark's DataFrame cache. The comparison still illustrates why repeated actions can be much cheaper once data has been read and made reusable. A separate count on the already small Delta subset was less informative: all of those actions took under a second.

## 3. Actions vs. transformations

The pipeline builds `ai_works` with `filter()`, `select()`, and `withColumn()`. Those transformations define a plan without processing all source records immediately. Printing the DataFrame and calling `explain()` lets me inspect its structure and planned operators. The following `count()` is the point where Spark actually launches the distributed computation. Later `show()` calls and Delta writes are actions as well. Seeing the plan before the count, then the timed job after it, made lazy evaluation much easier for me to understand.

## 4. Machine learning: MLlib clustering

As a small extension, I used `VectorAssembler` and MLlib `KMeans` to group the filtered papers by publication time and citations. The two features are `(publication_year - 2018) / 7` and `log1p(cited_by_count) / 7`; missing or negative citation values become zero when constructing the citation feature. I set `k=3`, `seed=13`, and `maxIter=20`, then saved the cluster summary as a Delta table.

| Cluster | Papers | Mean publication year | Mean citations |
| --- | ---: | ---: | ---: |
| 0 | 1,377 | 2020.0 | 4.4 |
| 1 | 2,521 | 2024.3 | 1.1 |
| 2 | 947 | 2022.1 | 38.9 |

The groups mainly reflect publication age and citation differences; they do not identify AI subfields or explain why some papers are cited more often. Newer papers have had less time to accumulate citations. [The MLlib execution screenshot](screenshots/ml_clusters.png) shows the successful run.

## Run the notebook and review the evidence

Open [`openalex_ai_pipeline.py`](openalex_ai_pipeline.py) as a Python notebook in Databricks, attach Serverless compute, and run the cells in order. The notebook reads the public OpenAlex paths above and writes the managed tables listed earlier to `workspace.default`. The source file uses Databricks `# COMMAND ----------` separators, so each stage can also be run and inspected separately.

| Evidence | What it shows |
| --- | --- |
| [Physical execution plan](screenshots/physical_plan.png) | The `.explain()` output and pushed filters on the Parquet scan |
| [Successful pipeline execution](screenshots/pipeline_success.png) | Six core Delta tables written and counted |
| [Databricks Query Details](screenshots/query_details.png) | The 26.8-second count, rows and bytes read, and task time |
| [Query Profile](screenshots/query_profile.png) | The complete Scan → Aggregate → Shuffle → Result operator graph |
| [Cache comparison](screenshots/cache_comparison.png) | Uncached, cache materialization, and repeated-action timings |
| [MLlib clustering](screenshots/ml_clusters.png) | Successful KMeans run and three-row cluster summary |

## Data references

- [OpenAlex snapshot access and structure](https://help.openalex.org/access/snapshot/)
- [OpenAlex Works attributes](https://help.openalex.org/data/works/attributes/)
- [OpenAlex Topics and hierarchy](https://help.openalex.org/data/topics/)
