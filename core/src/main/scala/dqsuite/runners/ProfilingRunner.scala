package dqsuite.runners

import com.amazon.deequ.suggestions.{ConstraintSuggestionResult, ConstraintSuggestionRunner, Rules}
import dqsuite.DQSuiteDatasetContext
import org.apache.spark.sql.DataFrame

/** The ProfilingRunner is responsible for running deequ profiling. The profiling runs general rule based analyzers on
  * the dataset and stores the metrics and check suggestions in the defined repositories.
  * @param context
  *   The context of the dataset to profile
  */
private[dqsuite] case class ProfilingRunner(
  context: DQSuiteDatasetContext
) {
  def run(
    df: DataFrame
  ): ConstraintSuggestionResult = {
    val suggestionsPath = context.resultPath.resolve("suggestions.json")
    val profilesPath    = context.resultPath.resolve("profiles.json")
    val evaluationPath  = context.resultPath.resolve("evaluation.json")

    ConstraintSuggestionRunner()
      .onData(df)
      .useSparkSession(context.suiteContext.spark)
      .overwritePreviousFiles(true)
      .saveConstraintSuggestionsJsonToPath(suggestionsPath.toString)
      .saveEvaluationResultsJsonToPath(evaluationPath.toString)
      .saveColumnProfilesJsonToPath(profilesPath.toString)
      .addConstraintRules(Rules.DEFAULT)
      .run()
  }
}
