from pyspark.sql import SparkSession, types as T, functions as F

from dqsuite import DQSuiteContextBuilder

spark = SparkSession.builder.getOrCreate()

source_file_path = "../data/iowa_liquor_sales_demo/iowa_liquor_sales_01.csv"
dq_config_path = "../data/dataquality.yml"
dq_output_path = "../out"
dq_profile = False
dq_anomaly_detection = True

# Set up Data Quality Context
dqsContext = (
    DQSuiteContextBuilder.builder(spark)
    .withConfigPath(dq_config_path)
    .withResultPath(f"{dq_output_path}/results")
    .withMetricsPath(f"{dq_output_path}/metrics")
    .withCloudwatchRepository("dqsuite/etl/example")
    .build()
)

# Load data
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(source_file_path)
)

# Raw Data Quality Checks
dsContext = dqsContext.withDataset("sales_raw")

schemaCheckResult = dsContext.checkSchema(df_raw)
# TODO: save invalid rows
assert schemaCheckResult.isValid, f"Schema Check Failed: {schemaCheckResult}"
df_raw_valid = schemaCheckResult.validRows  # there are no invalid rows, otherwise the check would fail

if dq_profile:
    profilingResult = dsContext.profile(df_raw_valid)
    print(f"Profiling finished. Used {profilingResult.numRecordsUsedForProfiling()} for profiling")

validationResult = dsContext.validate(df_raw_valid, anomalyDetection=dq_anomaly_detection)
assert validationResult.status != "Error", f"Data Quality Checks Failed: {validationResult.checkResults}"
print(f"Data Quality Checks Passed: {validationResult.status}")

df = dsContext.postprocess(df_raw_valid)

# Data Transformation
df = (
    df
    .filter(F.col("invoice_item_number").isNotNull())
    .dropDuplicates(["invoice_item_number"])
    .withColumn("zip_code", F.regexp_extract("zip_code", r"(\d{5})", 1))
    .withColumn("year", F.lpad(F.year("date"), 4, "0"))
    .withColumn("month", F.lpad(F.month("date"), 2, "0"))
    .withColumn("day", F.lpad(F.dayofmonth("date"), 2, "0"))
    .sortWithinPartitions("year", "month", "day")
)

# Output Data Quality Check
dsContext = dqsContext.withDataset("sales_processed")

schemaCheckResult = dsContext.checkSchema(df)
assert schemaCheckResult.isValid, f"Schema Check Failed: {schemaCheckResult.missingColumns}"
df = schemaCheckResult.validRows

validationResult = dsContext.validate(df, anomalyDetection=dq_anomaly_detection)
assert validationResult.status != "Error", f"Data Quality Checks Failed: {validationResult.checkResults}"
print(f"Data Quality Checks Passed: {validationResult.status}")
