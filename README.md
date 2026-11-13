# OpenAlex AI Research: PySpark Data Processing

Week 13 Mini Assignment. The pipeline follows the class demonstration's progression from a lazy DataFrame to an action, a physical plan, aggregations, a broadcast join, SQL queries, caching, and saved Delta tables.

## Dataset and research question

The source is the [OpenAlex public Parquet snapshot](https://help.openalex.org/access/snapshot/), dated **2026-10-14**. Works contain publication years, topics, authorships, institutions, countries, and citation counts. The [Works manifest](https://openalex.s3.amazonaws.com/data/parquet/works/manifest.json) reported 1,898 files and 744.0 GB. The [Topics manifest](https://openalex.s3.amazonaws.com/data/parquet/topics/manifest.json) reported six files totaling 5.6 MB.

The question is: **How are publication years, research topics, affiliated countries and educational institutions, and international collaboration represented among AI works in a reproducible OpenAlex file sample?** These are sample findings. The selected files cannot establish global publication totals or growth rates.

Eight Works files were sampled uniformly without replacement from the full manifest using Python `random.Random(13)`. Three of those files were sampled uniformly with `random.Random(1301)`. The final raw input is **2.02 GB compressed**, exceeding the assignment's 1 GB requirement:

```text
s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0008.parquet
s3://openalex/data/parquet/works/updated_date=2026-10-07/part_0011.parquet
s3://openalex/data/parquet/works/updated_date=2026-10-14/part_0084.parquet
```

OpenAlex partitions these files by the date a record was last updated, **not** its publication date. All three realized sample files have October 2026 update dates. Uniform file sampling gives each record an equal probability of selection at the file level, but three clusters leave substantial sampling uncertainty. The analysis is limited to publications from 2018 through 2025 whose primary topic belongs to the [Artificial Intelligence subfield (`1702`)](https://openalex.org/subfields/1702) and whose `is_xpac` value is false. API records were used to inspect the schema; the snapshot itself determines the reported results.

## Run the pipeline

Import or open [openalex_ai_pipeline.py](openalex_ai_pipeline.py) as a Databricks Python notebook and attach Serverless compute. Run the cells in order. The notebook reads the selected S3 Parquet files, writes a filtered AI subset as Delta, joins the small Topics lookup with a broadcast join, aggregates publication years, topics, countries, institutions, and international collaboration, runs two SQL queries, compares repeated counts with caching, and writes compact managed Delta tables under `workspace.default`.

The outputs are:

```text
workspace.default.openalex_ai_works
workspace.default.openalex_ai_annual
workspace.default.openalex_ai_topics
workspace.default.openalex_ai_countries
workspace.default.openalex_ai_institutions
workspace.default.openalex_ai_collaboration
```

The pipeline counts a paper once per country or educational institution, even when multiple coauthors share an affiliation. A multinational paper appears once for each represented country. The international collaboration share uses papers with at least one known affiliated country as its denominator. Citation averages are descriptive because newer publications have had less time to accumulate citations.

## Performance analysis

The initial filtered count completed in **27.0 seconds** and returned **4,845 AI works**. The observed `explain()` output showed a Photon Parquet scan over the three selected paths. Its `DataFilters` included the publication-year and `is_xpac` predicates, while `DictionaryFilters` included the AI subfield predicate. The scan reported `PartitionFilters: []`, as expected when reading exact file paths rather than a partition directory. The plan was fully supported by Photon. These plan labels show where Databricks applied filters; Query Details metrics are still needed to measure how many bytes or rows were skipped.

The base DataFrame's plan included the nested `authorships` field because that field is needed for the country and institution analyses. The annual aggregation does not use it, so its separate execution plan should be checked for column pruning and a grouping exchange. The main potential cost is scanning large remote Parquet files for a relatively small AI subset; a broad repartition before filtering would add a shuffle. The pipeline therefore filters before exploding authorships, saves the filtered result for reuse, broadcasts the small Topics lookup, and deduplicates country-paper and institution-paper pairs only after the AI filter. The cache cell times the uncached count, cache materialization, and a repeated cached count separately. Its measured benefit must be reported from a completed run because a cold Serverless start and remote I/O can affect timings.

## Findings and evidence status

- The first completed action found **4,845** AI works in the selected files under the stated filters.
- Annual, topic, country, institution, and collaboration tables require a completed run before their values can be reported.
- Databricks Query Details screenshots, a successful full-run screenshot, and a screenshot of the aggregation or join plan are still required. No screenshot is represented as completed evidence here.

When capturing the screenshots, use English notebook titles, cell labels, and captions. Add the files under `screenshots/` and link them here after verifying the displayed result and metrics.

## Sources

- [OpenAlex snapshot format and partitioning](https://help.openalex.org/access/snapshot/)
- [OpenAlex Works attributes](https://help.openalex.org/data/works/attributes/)
- [OpenAlex Topics and hierarchy](https://help.openalex.org/data/topics/)
