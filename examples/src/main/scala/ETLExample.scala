import com.amazon.deequ.checks.CheckStatus
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import dqsuite.DQSuiteContextBuilder
import org.apache.spark.SparkContext
import org.apache.spark.sql.{functions => F, types => T}

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object ETLExample {
  private val sparkContext: SparkContext = new SparkContext()
  private val glueContext: GlueContext   = new GlueContext(sparkContext)
  private val spark                      = glueContext.getSparkSession
  private val logger                     = new GlueLogger()

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

    // Load data
    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(args("input_file_path"))

    // Run schema check
    var dsContext         = dqsContext.withDataset("sales_raw")
    var schemaCheckResult = dsContext.checkSchema(dfRaw)
    logger.info(
      s"Schema check finished. Found ${schemaCheckResult.numInvalidRows} invalid rows" +
        s" and ${schemaCheckResult.numValidRows} valid rows"
    )
    if (schemaCheckResult.missingColumns.nonEmpty) {
      logger.warn(s"Missing columns: ${schemaCheckResult.missingColumns.mkString(", ")}")
    }
    if (!schemaCheckResult.isValid) {
      logger.warn(s"Invalid rows: ${schemaCheckResult.invalidRows.show(50)}")
    }
    assert(schemaCheckResult.isValid)

    // Use valid rows
    val df_raw_valid = schemaCheckResult.validRows

    // Run profiler and validator
//    val profilingResult = dsContext.profile(df_raw_valid)
//    logger.info(s"Profiling finished. Used ${profilingResult.numRecordsUsedForProfiling} for profiling")

    var validationResult = dsContext.validate(df_raw_valid)
    validationResult.status match {
      case CheckStatus.Success => logger.info("Validation succeeded")
      case CheckStatus.Warning => logger.warn("Validation succeeded with warnings")
      case CheckStatus.Error   => throw new RuntimeException("Validation failed")
    }

    var df = dsContext.postprocess(df_raw_valid)

    // Transform data
    df = (
      df.filter(F.col("invoice_item_number").isNotNull)
        .dropDuplicates(Seq("invoice_item_number"))
        .withColumn("zip_code", F.regexp_extract(F.col("zip_code"), "[0-9]{5}", 0))
        .withColumn("year", F.lpad(F.year(F.col("date")), 4, "0"))
        .withColumn("month", F.lpad(F.month(F.col("date")), 2, "0"))
        .withColumn("day", F.lpad(F.dayofmonth(F.col("date")), 2, "0"))
        .sortWithinPartitions(
          "year",
          "month",
          "day"
        )
    )

    // Run dataquality
    dsContext = dqsContext.withDataset("sales_processed")

    schemaCheckResult = dsContext.checkSchema(df)
    logger.info(
      s"Schema check finished. Found ${schemaCheckResult.numInvalidRows} invalid rows" +
        s" and ${schemaCheckResult.numValidRows} valid rows"
    )
    if (schemaCheckResult.missingColumns.nonEmpty) {
      logger.warn(s"Missing columns: ${schemaCheckResult.missingColumns.mkString(", ")}")
    }
    if (!schemaCheckResult.isValid) {
      logger.warn(s"Invalid rows: ${schemaCheckResult.invalidRows.show(50)}")
    }
    assert(schemaCheckResult.isValid)

    validationResult = dsContext.validate(df)
    validationResult.status match {
      case CheckStatus.Success => logger.info("Validation succeeded")
      case CheckStatus.Warning => logger.warn("Validation succeeded with warnings")
      case CheckStatus.Error   => throw new RuntimeException("Validation failed")
    }

  }
}
