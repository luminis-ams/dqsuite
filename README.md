## Data Quality Suite

A set of utilities for effortless data quality checks built on top of [deequ](https://github.com/awslabs/deequ).

Data Quality Suite (DQS) provides a configuration based approach to running data quality checks to ensure the rules are
decoupled from
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
* Python support

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

The suite is configured using a YAML file that contains configuration checks for multiple datasets / datasources.

See example below:

```yaml
sources:
  sales:
    analyzers:
      - column: "Dataset"
        expression: Size()
      - column: "Invoice/Item Number"
        expression: Completeness(@)::Uniqueness(@)
    checks:
      - column: "Dataset"
        level: error
        name: "Dataset must not be empty"
        expression: .hasSize(n => n > 0, Some("Dataset must not be empty"))
      - column: "Date"
        level: warning
        name: "Date validation"
        expression: >
          .hasPattern(@, "^[0-9]{4}-[0-9]{2}-[0-9]{2}$".r)
          .isComplete(@)
    anomaly_detection:
      - column: "Dataset"
        level: error
        description: "Dataset size must not be anomalous"
        expression: Size()
        strategy: OnlineNormalStrategy(lowerDeviationFactor = Some(3.0), upperDeviationFactor = Some(3.0))
        window: 604800
```

* **Sources**: Datasource configurations are grouped under the `sources` key with a unique name for each datasource used
  during the run. Each datasource has configuration for **analyzers**, **checks** and **anomaly_detection**.

* **Analyzers**: Analyzers are used to produce metrics that can be used for anomaly detection.
  * **Column**: Column name to run the analyzer on.
  * **Expression**: Deequ analyzer expression to run on the column. (`@` is substituted with the column name)
    * Use :: to chain multiple analyzers.
    * **Enabled**: Whether to run the check or not. (default: true)
    * See [deequ Analyzers](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/analyzers) for options.
* **Checks**: Checks are used to validate the data quality.
  * **Column**: Column name to run the check on.
  * **Level**: Level of the check. (error, warning, info)
  * **Name**: Name of the check.
  * **Expression**: Deequ check expression to run on the column. (`@` is substituted with the column name)
  * **Enabled**: Whether to run the check or not. (default: true)
    * See [deequ Checks](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/checks/Check.scala) for options.
* **Anomaly Detection**: Anomaly detection is used to detect anomalies in the metrics produced by the analyzers. It acts as an additional check.
  * **Column**: Column name to run the check on.
  * **Level**: Level of the check. (error, warning, info)
  * **Description**: Description of the anomaly.
  * **Expression**: Deequ analyzer expression to run on the column. (`@` is substituted with the column name)
  * **Strategy**: Strategy to use for anomaly detection.
    * See [deequ Anomaly Detection Strategies](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/anomalydetection) for options
  * **Window**: Window (in seconds) to use for historical metric gathering. (default: infinite)
  * **Enabled**: Whether to run the check or not. (default: true)

## Deployment

### Glue Scala ETL Script

1. Place the `dataquality-suite-bundle_2.12-0.1.jar` and `deequ-2.0.3-spark-3.3.jar` jars in your scrips S3 bucket.
2. Create a new Glue job, select the Scala script and specify your script sources.
3. Add following job parameters so glue loads the libs at runtime:

```json
{
  "--extra-jars": "s3://<bucket>/dataquality-suite-bundle_2.12-0.1.jar,s3://<bucket>/deequ-2.0.3-spark-3.3.jar"
}
```

4. See `examples` folder for examples of how to use the suite.

### Glue Python ETL Script

If you want to use the dataquality suite in your scripts you need additional python dependencies.

1. Place the `dataquality-suite-bundle_2.12-0.1.jar` and `deequ-2.0.3-spark-3.3.jar` jars in your scrips S3 bucket.
2. Place the `dqsuite-0.1.0-py3-none-any.whl` in your scrips S3 bucket.
3. Create a new Glue job, select the Python script and specify your script sources.
4. Add following job parameters so glue loads the libs at runtime:

```json
{
  "--extra-jars": "s3://<bucket>/dataquality-suite-bundle_2.12-0.1.jar,s3://<bucket>/deequ-2.0.3-spark-3.3.jar"
  "--additional-python-modules": "s3://<bucket>/dqsuite-0.1.0-py3-none-any.whl"
}
```

5. See `python/examples` folder for examples of how to use the suite.

## Backlog

* [ ] Schema Checking
* [x] Run 2022 partitions through analyzer
* [x] Add configurable repository
* [x] Anomaly Detection configuration
* [x] Load configuration from the S3 path
* [x] Save check results to S3
* [x] Save metric results to Timestream and/or S3
* [x] Python support
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