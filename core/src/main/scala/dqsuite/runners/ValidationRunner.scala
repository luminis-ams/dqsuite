package dqsuite.runners
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import dqsuite.DQSuiteDatasetContext
import dqsuite.deequ.{AnomalyDetectionInstance, DeequAnalyzerFactory, DeequAnomalyDetectorFactory, DeequCheckFactory}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame

import java.time.Instant

/** The ValidationRunner is responsible for quality metrics collection, checks execution and anomaly detection.
  * @param context
  *   The context of the dataset to validate
  * @param anomalyDetection
  *   Whether to run anomaly detection or not
  */
private[dqsuite] case class ValidationRunner(
  context: DQSuiteDatasetContext,
  anomalyDetection: Boolean = true
) {
  private val logger = LogManager.getLogger()

  def run(
    df: DataFrame
  ): VerificationResult = {
    // Instantiate deequ objects for checks, analyzers and anomaly detectors.
    val checks = DeequCheckFactory.buildSeq(context.config)
    if (checks.isEmpty) {
      logger.warn("No checks found for dataset")
    }

    val analyzers = DeequAnalyzerFactory.buildSeq(context.config)
    if (analyzers.isEmpty) {
      logger.warn("No analyzers found for dataset")
    }

    val anomalyDetectors = DeequAnomalyDetectorFactory.buildSeq(
      Some(Instant.ofEpochMilli(context.resultKey.dataSetDate)),
      context.config,
      Map("dataset" -> context.resultKey.tags("dataset"))
    )
    if (anomalyDetectors.isEmpty) {
      logger.warn("No anomaly detectors found for dataset")
    }

    if (checks.isEmpty && analyzers.isEmpty && anomalyDetectors.isEmpty) {
      return VerificationResult(
        CheckStatus.Success,
        Map(),
        Map()
      )
    }

    val checksPath = context.resultPath.resolve("checks.json")

    // Run deequ validation and store results in defined repositories.
    logger.info("Running validation")
    var suite = VerificationSuite()
      .onData(df)
      .useSparkSession(context.suiteContext.spark)
      .overwritePreviousFiles(true)
      .saveCheckResultsJsonToPath(checksPath.toString)
      .addChecks(checks)
      .addRequiredAnalyzers(analyzers)
      .useRepository(context.repository)
      .reuseExistingResultsForKey(context.resultKey, failIfResultsMissing = false)
      .saveOrAppendResult(context.resultKey)

    // Hack to get around type erasure.
    def addAnomalyCheck[S <: State[S]](instance: AnomalyDetectionInstance) = {
      suite.addAnomalyCheck(
        instance.strategy,
        instance.analyser.asInstanceOf[Analyzer[S, Metric[Double]]],
        Some(instance.config)
      )
    }

    // Do a check so runs where there is no data yet are not anomalous.
    if (anomalyDetectors.nonEmpty && anomalyDetection) {
      for (instance <- anomalyDetectors) {
        suite = addAnomalyCheck(instance)
      }
    }
    val result = suite.run()

    logger.info("Saving metrics to ephemeral repositories")
    for (repo <- context.suiteContext.empheralRespositories) {
      repo.save(context.resultKey, AnalyzerContext(result.metrics))
    }

    result
  }
}
