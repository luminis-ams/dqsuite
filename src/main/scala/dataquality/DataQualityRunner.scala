package dataquality

import com.amazon.deequ.analyzers.{Analyzer, HdfsStateProvider, State, StatePersister}
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import dataquality.config.DataQualityConfig.loadStream
import dataquality.config.{DataQualityConfig, SourceConfig}
import dataquality.respository.timestream.TimestreamMetricsRepositoryBuilder
import dataquality.utils.HdfsUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import java.net.URI
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object DataQualityRunner {
  private val sparkContext: SparkContext = new SparkContext()
  private val glueContext: GlueContext = new GlueContext(sparkContext)
  private val spark = glueContext.getSparkSession
  private val logger = new GlueLogger()

  def main(sysArgs: Array[String]): Unit = {
    val args = ArgParser.parse(sysArgs)

    val glueArgs = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(glueArgs("JOB_NAME"), glueContext, glueArgs.asJava)

    // Load configuation
    val config = HdfsUtils.readFromFileOnDfs(spark, args.configPath)(DataQualityConfig.loadStream)
    val sourceConfig = config.sources.get(args.sourceName) match {
      case Some(config) => config
      case None         => throw new RuntimeException(s"Source ${args.sourceName} not found in config")
    }

    // Create result key (for storing metrics).
    val resultKey = ResultKey(
      args.datasetTimestamp.toEpochMilli,
      Map(
        "source" -> args.sourceName,
        "run_name" -> args.runName,
      ) ++ args.partition.map("partition" -> _) ++ sourceConfig.tags
    )

    val resultPath = args.resultPath.resolve(s"${args.sourceName}/").resolve(s"${args.runName}/")
    val metricsPath = args.metricsPath.resolve(s"${args.sourceName}/")

    val dyf = loadData(sourceConfig, args)
    val df = dyf.toDF()

    if (args.actions.contains(Action.PROFILE)) {
      logger.info("Running profiler")
      runProfiler(df, resultPath)
      logger.info("Profiling finished")
    }
    if (args.actions.contains(Action.VALIDATE)) {
      logger.info("Running validator")
      val metricsRepo = FileSystemMetricsRepository(spark, metricsPath.resolve("repository/").resolve("metrics.json").toString)
      val timestreamRepo = args.timestreamRepository.map(repoArgs => TimestreamMetricsRepositoryBuilder.builder
        .useTable(repoArgs.database, repoArgs.table)
        .build
      )
      val statePersister = new HdfsStateProvider(
        spark,
        metricsPath.resolve("state/").toString,
        numPartitionsForHistogram = 10,
        allowOverwrite = true
      )
      // Identifier is the metric name. Thus we need better namespacing.
      // Add a staging and final aggregated state. Flip between two is changes are not quarantined.

      val result = runValidator(df, resultKey, resultPath, metricsRepo, statePersister, sourceConfig)
      result.map(r => r.status) match {
        case Some(CheckStatus.Success) => logger.info("Validation succeeded")
        case Some(CheckStatus.Warning) => logger.warn("Validation succeeded with warnings")
        case Some(CheckStatus.Error)   => throw new RuntimeException("Validation failed")
      }

//      statePersister.
    }

    Job.commit()
  }

  def loadData(
    config: SourceConfig,
    args: Args,
  ): DynamicFrame = {
    args.dataSource match {
      case GlueTableDataSourceArgs(database, table) =>
        glueContext
          .getCatalogSource(
            database,
            table,
            transformationContext = "dq_source",
          )
          .getDynamicFrame()
      case FilesystemDataSourceArgs(path) =>
        path.startsWith("s3://") match {
          case true =>
            glueContext
              .getSourceWithFormat(
                connectionType = "s3",
                options = JsonOptions(Map("paths" -> Seq(path))),
                format = config.format.get,
                formatOptions = JsonOptions(config.glueOptions.getOrElse("{}")),
                transformationContext = "dq_source",
              )
              .getDynamicFrame()
          case false =>
            DynamicFrame(
              spark.read
                .options(config.sparkOptions.getOrElse(Map()))
                .format(config.format.get)
                .load(path),
              glueContext,
            )
        }
    }
  }

  private def runProfiler(
    df: DataFrame,
    resultPath: URI,
  ) = {
    ConstraintSuggestionRunner()
      .onData(df)
      .useSparkSession(spark)
      .overwritePreviousFiles(true)
      .saveConstraintSuggestionsJsonToPath(resultPath.resolve("suggestions.json").toString)
      .saveEvaluationResultsJsonToPath(resultPath.resolve("evaluation.json").toString)
      .saveColumnProfilesJsonToPath(resultPath.resolve("profiles.json").toString)
      .addConstraintRules(Rules.DEFAULT)
      .run()
  }

  def runValidator(
    df: DataFrame,
    resultKey: ResultKey,
    resultPath: URI,
    repository: MetricsRepository,
    statePersister: StatePersister,
    sourceConfig: SourceConfig,
  ): Option[VerificationResult] = {
    logger.info("Running validator")

    val checks = DeequFactory.buildChecks(sourceConfig)
    if (checks.isEmpty) {
      logger.warn("No checks found for source")
    }

    val analyzers = DeequFactory.buildAnalyzers(sourceConfig)
    if (analyzers.isEmpty) {
      logger.warn("No analyzers found for source")
    }

    val anomalyDetectors = DeequFactory.buildAnomalyDetectors(
      sourceConfig, Map("source" -> resultKey.tags("source"))
    )
    if (anomalyDetectors.isEmpty) {
      logger.warn("No anomaly detectors found for source")
    }

    if (checks.isEmpty && analyzers.isEmpty && anomalyDetectors.isEmpty) {
      return None
    }

    var suite = VerificationSuite()
      .onData(df)
      .useSparkSession(spark)
      .overwritePreviousFiles(true)
      .saveCheckResultsJsonToPath(resultPath.resolve("checks.json").toString)
      .addChecks(checks)
      .addRequiredAnalyzers(analyzers)
      .useRepository(repository)
      .saveStatesWith(statePersister)
      .reuseExistingResultsForKey(resultKey, failIfResultsMissing = false)
      .saveOrAppendResult(resultKey)

    // Hack to get around type erasure.
    def addAnomalyCheck[S <: State[S]](instance: AnomalyDetectionInstance) = {
      suite.addAnomalyCheck(instance.strategy, instance.analyser.asInstanceOf[Analyzer[S, Metric[Double]]], Some(instance.config))
    }

    for (instance <- anomalyDetectors) {
      suite = addAnomalyCheck(instance)
    }

    val result = suite.run()

    Some(result)
  }
}
