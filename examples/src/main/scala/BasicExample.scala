import com.amazon.deequ.checks.CheckStatus
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import dqsuite.DQSuiteContextBuilder
import org.apache.spark.SparkContext
import org.apache.spark.sql.{functions => F, types => T}

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object BasicExample {
  private val sparkContext: SparkContext = new SparkContext()
  private val glueContext: GlueContext = new GlueContext(sparkContext)
  private val spark = glueContext.getSparkSession
  private val logger = new GlueLogger()

  def main(sysArgs: Array[String]): Unit = {
    // * config_path: Path to the data quality configuration file. Can be a local file or a S3 URI.
    // * input_file_path: Path to the input file. Can be a local file or a S3 URI.
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "input_file_path", "config_path").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    // Configure DQSuite
    val dqsContext = DQSuiteContextBuilder.builder
      .withConfigPath(args("config_path"))
      .withResultPath("./out/results")
      .withMetricsPath("./out/metrics")
      .withSparkSession(spark)
      .build

    val dsContext = dqsContext.withDataset("sales")

    // Load data
    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(args("input_file_path"))
      .withColumn("date", F.col("date").cast(T.StringType))

    // Run schema check
    val schemaCheckResult = dsContext.schemaCheck(dfRaw)
    logger.info(
      s"Schema check finished. Found ${schemaCheckResult.numInvalidRows} invalid rows" +
        s" and ${schemaCheckResult.numValidRows} valid rows")

    // Use valid rows
    val df = schemaCheckResult.validRows

    // Run profiler and validator
    val profilingResult = dsContext.profile(df)
    logger.info(s"Profiling finished. Used ${profilingResult.numRecordsUsedForProfiling} for profiling")

    val validationResult = dsContext.validate(df)
    validationResult.status match {
      case CheckStatus.Success => logger.info("Validation succeeded")
      case CheckStatus.Warning => logger.warn("Validation succeeded with warnings")
      case CheckStatus.Error   => throw new RuntimeException("Validation failed")
    }

    val dyf = DynamicFrame(df, glueContext)
    dyf.printSchema()
  }
}
