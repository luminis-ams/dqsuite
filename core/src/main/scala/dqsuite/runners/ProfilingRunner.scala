package dqsuite.runners

import com.amazon.deequ.suggestions.{ConstraintSuggestionResult, ConstraintSuggestionRunner, Rules}
import dqsuite.DQSuiteDatasetContext
import org.apache.spark.sql.DataFrame

private[dqsuite] case class ProfilingRunner(
  context: DQSuiteDatasetContext,
) {
  def run(
    df: DataFrame
  ): ConstraintSuggestionResult = {
    val suggestionsPath = context.resultPath.resolve("suggestions.json")
    val profilesPath = context.resultPath.resolve("profiles.json")
    val evaluationPath = context.resultPath.resolve("evaluation.json")

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
