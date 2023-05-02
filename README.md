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
    schema:
      - column: "Invoice/Item Number"
        type: string
        is_nullable: false
      - column: "date"
        type: timestamp
        mask: "yyyy-MM-dd"
      - column: "Volume Sold (Liters)"
        type: decimal
        precision: 12
        scale: 2
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
  during the run. Each datasource has configuration for **schema**, **analyzers**, **checks** and **anomaly_detection**.

### Schema

Schema is used to check input data against the expected schema.

* **column**: Column name to run the check on.
* **required**: Whether the column is required or not. (default: true)
* **is_nullable**: Whether the column can be null or not. (default: true)
* **type**: Type of the column. (string, integer, decimal, timestamp, expression)
* **alias**: Used in `postprocess` to rename the column. Not used during schema check.
* For string columns:
    * **min_length** (optional): Minimum length of the string column. (default: 0)
    * **max_length** (optional): Maximum length of the string column. (default: Infty)
    * **matches** (optional): Regex pattern to check the data against.. (default: None)
* For integer columns:
    * **min_value** (optional): Minimum value of the integer column. (default: 0)
    * **max_value** (optional): Maximum value of the integer column. (default: Infty)
* For decimal columns:
    * **precision**: Precision of the decimal column.
    * **scale**: Scale of the decimal column.
* For timestamp columns:
    * **mask**: Mask to used to parse the timestamp column.
* For expression columns:
    * **expression**: A valid deequ `RowLevelSchema` method.

### Analyzers

Analyzers are used to produce metrics that can be used for anomaly detection.

* **column**: Column name to run the analyzer on.
* **expression**: Deequ analyzer expression to run on the column. (`@` is substituted with the column name)
    * Use :: to chain multiple analyzers.
    * **enabled**: Whether to run the check or not. (default: true)
    * See [deequ Analyzers](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/analyzers) for
      options.

### Checks

Checks are used to validate the data quality.

* **column**: Column name to run the check on.
* **level**: Level of the check. (error, warning, info)
* **name**: Name of the check.
* **expression**: Deequ check expression to run on the column. (`@` is substituted with the column name)
* **enabled**: Whether to run the check or not. (default: true)
    * See [deequ Checks](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/checks/Check.scala)
    for options.

### Anomaly Detection

Anomaly detection is used to detect anomalies in the metrics produced by the analyzers. It acts as an additional check.

* **column**: Column name to run the check on.
* **level**: Level of the check. (error, warning, info)
* **description**: Description of the anomaly.
* **expression**: Deequ analyzer expression to run on the column. (`@` is substituted with the column name)
* **strategy**: Strategy to use for anomaly detection.
    * See [deequ Anomaly Detection Strategies](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/anomalydetection)
    for options
* **window**: Window (in seconds) to use for historical metric gathering. (default: infinite)
* **enabled**: Whether to run the check or not. (default: true)

## Usage

### Scala

Create the context by providing additional parameters such as result paths and repository confgiuration:

```scala
// Configure DQSuite
val dqsContext = DQSuiteContextBuilder.builder
  .withConfigPath(args("config_path"))
  .withResultPath("./out/results")
  .withMetricsPath("./out/metrics")
  .withSparkSession(spark)
  .build
```

Select the source / source configuration you will be using for profiling:

```scala
val dsContext = dqsContext.withDataset("sales")
```

Check schema and data types of your data:

```scala
val schemaCheckResult = dsContext.checkSchema(dfRaw)
assert(schemaCheckResult.isValid)
```

Data profiling computes metrics on your data inferred using some basic rules and data schema.
It emits these profiling results as well as some suggestions on checks you may want to configure.

```scala
val profilingResult = dsContext.profile(df)
logger.info(s"Profiling finished. Used ${profilingResult.numRecordsUsedForProfiling} for profiling")
```

Validation runs configured metrics, checks and anomaly detection against your data. Run it using:

```scala
val validationResult = dsContext.validate(df)
validationResult.status match {
  case CheckStatus.Success => logger.info("Validation succeeded")
  case CheckStatus.Warning => logger.warn("Validation succeeded with warnings")
  case CheckStatus.Error => throw new RuntimeException("Validation failed")
}
```

Finally, you can apply postprocessing for quick data transformation. Following transformations are supported:
* Column renaming: (if `alias` is specified in schema config)

```scala
val dfPostProcessed = dsContext.postprocess(df)
```

### Python

Create the context by providing additional parameters such as result paths and repository confgiuration:

```python
# Configure DQSuite
dqsContext = (
    DQSuiteContextBuilder.builder(spark)
    .withConfigPath(config_path)
    .withResultPath("./out/results")
    .withMetricsPath("./out/metrics")
    .build()
)
```

Select the source / source configuration you will be using for profiling:

```python
dsContext = dqsContext.withDataset("sales")
```

Check schema and data types of your data:

```python
schemaCheckResult = dsContext.checkSchema(dfRaw)
assert schemaCheckResult.isValid
```

Data profiling computes metrics on your data inferred using some basic rules and data schema.
It emits these profiling results as well as some suggestions on checks you may want to configure.

```python
profilingResult = dsContext.profile(df)
print(f"Profiling finished. Used {profilingResult.numRecordsUsedForProfiling} for profiling")
```

Validation runs configured metrics, checks and anomaly detection against your data. Run it using:

```python
validationResult = dsContext.validate(df)
if validationResult.status == "Error":
    raise Exception("Validation failed")
elif validationResult.status == "Warning":
    print("Validation succeeded with warnings")
else:
    print("Validation succeeded")
```

Finally, you can apply postprocessing for quick data transformation. Following transformations are supported:
* Column renaming: (if `alias` is specified in schema config)

```python
dfPostProcessed = dsContext.postprocess(df)
```

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

* [x] Schema Checking
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

## Publishing

### Python
```shell
cd python
twine upload --repository codeartifact dist/dqsuite-0.1.0-py3-none-any.whl
```


### Scala
```shell

```