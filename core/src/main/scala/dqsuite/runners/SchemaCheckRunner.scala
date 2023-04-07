package dqsuite.runners

import com.amazon.deequ.schema.{RowLevelSchemaValidationResult, RowLevelSchemaValidator}
import dqsuite.DQSuiteDatasetContext
import dqsuite.deequ.DeequSchemaFactory
import org.apache.spark.sql.DataFrame

case class SchemaCheckRunner(
  context: DQSuiteDatasetContext,
) {

  def run(df: DataFrame): RowLevelSchemaValidationResult = {
    val schema = DeequSchemaFactory.buildSeq(context.config)
    RowLevelSchemaValidator
      .validate(df, schema)
  }

}
