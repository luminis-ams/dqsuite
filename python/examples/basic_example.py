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

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv('../examples/data/iowa_liquor_sales_demo/iowa_liquor_sales_01.csv')
    .withColumn("date", F.col("date").cast(T.StringType()))
)

profilingResult = dsContext.profile(df)
print(f"Profiling finished. Used {profilingResult.numRecordsUsedForProfiling} for profiling")

validationResult = dsContext.validate(df)
print(validationResult.status)