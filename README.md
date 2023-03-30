## Backlog

* [ ] Schema Checking
* [x] Run 2022 partitions through analyzer
* [x] Add configurable repository
* [ ] Anomaly Detection configuration
* [x] Load configuration from the S3 path
* [ ] State saving and aggregation
* [ ] Save check results to DynamoDB and/or S3
* [ ] Save metric results to Timestream and/or S3
* [ ] Describe flow for both incremental and full refresh datasets
* [ ] Support ELK stack as a repository for observability

Data quarantining within the repos?

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

## Data Sources

### Glue Table


### Filesystem


## Data Processing Patterns

### Full Refresh

A full refresh is a data processing pattern where the entire dataset is processed and stored.

Example parameter configuration:

```json
```

### Incremental

An incremental refresh is a data processing pattern where only the new data is processed and stored.

> A lot of incremental pipelines apply UPSERT logic to the data.
> This should be handled with care as this can cause drift in stateful metric aggregations.


Example parameter configuration:

```json
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