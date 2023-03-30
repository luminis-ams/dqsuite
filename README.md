## Data Quality Suite

## Features
* Data Profiling and suggestions for Data Quality Checks
* Anomaly Detection
* Decoupled per datasource Metric and Check configuration
* Storage of results on S3 or Timestream for building observability dashboards
* Load data from Glue Tables, S3, HDFS or local filesystem

## Parameters

* `config_path`: Path to the data quality configuration file. Can be a local file or a S3 URI.
* `source_name`: Name of the source as defined in the configuration file.
* `actions`: Comma separated list of actions to perform. Possible values are:
    * `PROFILE`: Runs the profiler and stores the results and suggestions.
    * `VALIDATE`: Runs the analyzers and checks, and stores the results.
* `metrics_path`: Path to use for filesystem metrics repository. Can be a local dir or a S3 URI.
* `result_path`: Path to store the results. Can be a local dir or a S3 URI.
* `run_name`: Name of the run. Used for saving results. Leave empty to use current timestamp. (may not include : or /)
* `partition`: Partition to use for the run. Used for tagging results. Optional.
* `dataset_timestamp`: Timestamp of the dataset. Used for incremental datasets. Leave empty to use current timestamp.

If you use AWS Glue as the data source, the following parameters are required:

* `glue_database`: Name of the Glue database to use.
* `glue_table`: Name of the Glue table to use.

If you use File System as the data source, the following parameters are required:

* `input_file_path`: Path to the input file. Can be a local file or a S3 URI.

If you use timestream as the metric repository, the following parameters are required:

* `timestream_database`: Name of the Timestream database to use.
* `timestream_table`: Name of the Timestream table to use.

### Example:

```json
{
  "--config_path": "s3://glue-jobs-domain/dataquality/config.yml",
  "--result_path": "s3://glue-output-domain/dataquality/results",
  "--metrics_path": "s3://glue-output-domain/dataquality/metrics",
  "--input_file_path": "s3://raw-zone-domain/sales/landing/iowa_liquor_sales_01.csv",
  "--source_name": "sales",
  "--actions": "VALIDATE",
  "--dataset_timestamp": "2022-09-01T00:00:00.00Z"
}
```

## Data Sources

### Glue Table

To import data from a Glue table, the following parameters are required:

* `glue_database`: Name of the Glue database to use.
* `glue_table`: Name of the Glue table to use.

### Filesystem

To import data from a file, the following parameters are required:

* `input_file_path`: Path to the input file. Can be a local file or a S3 URI.

Make sure to specify the correct file format and optionally the format options in the config:

```yaml
sources:
  sales:
    format: csv
    spark_options:
      inferSchema: "true"
      header: "true"
```

## Data Processing Patterns

### Full Refresh

A full refresh is a data processing pattern where the entire dataset is processed and stored.

Example parameter configuration:

```json
{
}
```

### Incremental

An incremental refresh is a data processing pattern where only the new data is processed and stored.

> A lot of incremental pipelines apply UPSERT logic to the data.
> This should be handled with care as this can cause drift in stateful metric aggregations.


Example parameter configuration:

```json
{
}
```

## Storage

### Metric Storage

To store metrics the **repository** concept from [deequ] is used.
Metrics are produced by both **profiler** and the **analyzer**.

There are two types of repositories that can be used:

* **Persistent Repository**: stores metrics persistently and can be used for retrieval when using features such as
  anomaly detection.
    * `FileSystemMetricsRepository`: Can store metrics in a local file system, HDFS or S3.
* **Empheral Repository**: stores metrics in storage that does not support their retrieval within [deequ] across runs.
    * `InMemoryMetricsRepository`: Stores metrics in memory.
    * `TimestreamMetricsRepository`: Stores metrics in AWS Timestream.

### Result Storage

The results include check results and check suggestion results.

These can be stored using:

* `FileSystemResultPersistor`: Can store results in a local file system, HDFS or S3.

## Configuration

## Deployment

## Backlog

* [ ] Schema Checking
* [x] Run 2022 partitions through analyzer
* [x] Add configurable repository
* [x] Anomaly Detection configuration
* [x] Load configuration from the S3 path
* [x] Save check results to S3
* [x] Save metric results to Timestream and/or S3
* [ ] Describe flow for both incremental and full refresh datasets
* [ ] Support ELK stack as a repository for observability
  * [ ] Add metric publishing to open search (use repos or lambdas (external)?)

### State Saving

Status: **Will not implement** (for now)

Saving states and aggregation is supported but tricky to implement together with data quarantining principle.

One way one would implement this is to have live and staging aggregated states.
1. Live state is used for aggregating metrics with the current state.
2. The aggregated results are stored in the staging state.
3. Once the whole ELT process is complete (and data is published), the staging state is copied to the live state.

The caveat is that this doesn't work with parallel execution.