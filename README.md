## Data Quality Suite

A set of utilities for effortless data quality checks built on top of [deequ](https://github.com/awslabs/deequ).

Data Quality Suite (DQS) provides a configuration based approach to running data quality checks to ensure the rules are decoupled from
transformation logic.

The suite is designed to be used with data quarantining principle in mind. 
Meaning that the bad data is caught early and is quarantined to avoid breaking the downstream systems. 
This required proper protocols for handling of quarantined data by raising issues and notifying the data owners.


## Features
To support above mentioned principles, the suite provides the following features:

* Data Profiling and Data Quality Check suggestions
* Anomaly Detection
* Storage of Quality Metrics on S3 or Timestream for observability
* Decoupled per dataset Metric and Check configuration
* Runtime agnostic data quality utilities

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