package dqsuite
import com.amazon.deequ.VerificationResult
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.suggestions.ConstraintSuggestionResult
import dqsuite.config.SourceConfig
import dqsuite.runners.{ProfilingRunner, SchemaCheckResult, SchemaCheckRunner, ValidationRunner}
import org.apache.spark.sql.DataFrame

import java.net.URI

case class DQSuiteDatasetContext(
  suiteContext: DQSuiteContext,
  config: SourceConfig,
  metricsPath: URI,
  resultPath: URI,
  resultKey: ResultKey,
  repository: MetricsRepository,
) {
  def profile(
    df: DataFrame,
  ): ConstraintSuggestionResult = {
    ProfilingRunner(this).run(df)
  }

  def checkSchema(
    df: DataFrame,
  ): SchemaCheckResult = {
    SchemaCheckRunner(this).run(df)
  }

  def validate(
    df: DataFrame,
    anomalyDetection: Boolean = true,
  ): VerificationResult = {
    ValidationRunner(this, anomalyDetection).run(df)
  }
}
