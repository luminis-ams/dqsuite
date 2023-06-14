package dqsuite.runners

import org.apache.spark.sql.{functions => F}
import dqsuite.DQSuiteDatasetContext
import org.apache.spark.sql.DataFrame

private[dqsuite] case class PostprocessRunner(
  context: DQSuiteDatasetContext
) {

  def run(
    df: DataFrame
  ): DataFrame = {
    // Rename columns
    val schemaColumnAliases = context.config.schema
      .getOrElse(Seq.empty)
      .filter(_.alias.isDefined)
      .map(x => x.column -> x.alias.get)
      .toMap

    df.select(
      df.columns.map(col => F.col(col).as(schemaColumnAliases.getOrElse(col, col))): _*
    )
  }

}
