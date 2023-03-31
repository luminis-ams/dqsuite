package dqsuite
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.suggestions.{ConstraintSuggestionResult, ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import dqsuite.config.SourceConfig
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame

import java.net.URI
import java.time.Instant

case class DQSuiteDatasetContext(
  context: DQSuiteContext,
  config: SourceConfig,
  metricsPath: URI,
  resultPath: URI,
  resultKey: ResultKey,
  repository: MetricsRepository,
) {
  private val logger = LogManager.getLogger()

  def profile(
    df: DataFrame,
  ): ConstraintSuggestionResult = {
    val suggestionsPath = resultPath.resolve("suggestions.json")
    val profilesPath = resultPath.resolve("profiles.json")
    val evaluationPath = resultPath.resolve("evaluation.json")

    ConstraintSuggestionRunner()
      .onData(df)
      .useSparkSession(context.spark)
      .overwritePreviousFiles(true)
      .saveConstraintSuggestionsJsonToPath(suggestionsPath.toString)
      .saveEvaluationResultsJsonToPath(evaluationPath.toString)
      .saveColumnProfilesJsonToPath(profilesPath.toString)
      .addConstraintRules(Rules.DEFAULT)
      .run()
  }

  def validate(
    df: DataFrame,
  ): VerificationResult = {
    logger.info("Running validator")

    val checks = DeequFactory.buildChecks(config)
    if (checks.isEmpty) {
      logger.warn("No checks found for dataset")
    }

    val analyzers = DeequFactory.buildAnalyzers(config)
    if (analyzers.isEmpty) {
      logger.warn("No analyzers found for dataset")
    }

    val anomalyDetectors = DeequFactory.buildAnomalyDetectors(
      Some(Instant.ofEpochMilli(resultKey.dataSetDate)),
      config,
      Map("dataset" -> resultKey.tags("dataset"))
    )
    if (anomalyDetectors.isEmpty) {
      logger.warn("No anomaly detectors found for dataset")
    }

    if (checks.isEmpty && analyzers.isEmpty && anomalyDetectors.isEmpty) {
      return VerificationResult(
        CheckStatus.Success,
        Map(),
        Map(),
      )
    }

    val checksPath = resultPath.resolve("checks.json")

    var suite = VerificationSuite()
      .onData(df)
      .useSparkSession(context.spark)
      .overwritePreviousFiles(true)
      .saveCheckResultsJsonToPath(checksPath.toString)
      .addChecks(checks)
      .addRequiredAnalyzers(analyzers)
      .useRepository(repository)
      .reuseExistingResultsForKey(resultKey, failIfResultsMissing = false)
      .saveOrAppendResult(resultKey)

    // Hack to get around type erasure.
    def addAnomalyCheck[S <: State[S]](instance: AnomalyDetectionInstance) = {
      suite.addAnomalyCheck(instance.strategy,
                            instance.analyser.asInstanceOf[Analyzer[S, Metric[Double]]],
                            Some(instance.config))
    }

    for (instance <- anomalyDetectors) {
      suite = addAnomalyCheck(instance)
    }
    val result = suite.run()

    logger.info("Saving metrics to ephemeral repositories")
    for (repo <- context.empheralRespositories) {
      repo.save(resultKey, AnalyzerContext(result.metrics))
    }

    result
  }
}
