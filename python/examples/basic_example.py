from pyspark.sql import SparkSession, types as T, functions as F

from dqsuite import DQSuiteContextBuilder

config_path = '../examples/data/example.yml'

spark = SparkSession.builder.getOrCreate()

dqsContext = (
    DQSuiteContextBuilder.builder(spark)
    .withConfigPath(config_path)
    .withResultPath("./out/results")
    .withMetricsPath("./out/metrics")
    .build()
)
dsContext = dqsContext.withDataset("sales")

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv('../examples/data/iowa_liquor_sales_demo/iowa_liquor_sales_01.csv')
    .withColumn("date", F.col("date").cast(T.StringType()))
)

schemaCheckResult = dsContext.checkSchema(df_raw)
print(f"Schema check finished. Found {schemaCheckResult.numInvalidRows} invalid rows" +
      f" and {schemaCheckResult.numValidRows} valid rows")
assert int(schemaCheckResult.numInvalidRows) == 0, "Schema check failed"
df = schemaCheckResult.validRows

profilingResult = dsContext.profile(df)
print(f"Profiling finished. Used {profilingResult.numRecordsUsedForProfiling()} for profiling")

validationResult = dsContext.validate(df)
print(validationResult.status)
if validationResult.status == "Error":
    raise Exception("Validation failed")
elif validationResult.status == "Warning":
    print("Validation succeeded with warnings")
else:
    print("Validation succeeded")
print(validationResult.checkResults)