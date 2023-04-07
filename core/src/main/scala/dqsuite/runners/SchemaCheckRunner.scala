package dqsuite.runners

import org.apache.spark.sql.{functions => F}
import com.amazon.deequ.schema.{RowLevelSchemaValidationResult, RowLevelSchemaValidator}
import dqsuite.DQSuiteDatasetContext
import dqsuite.deequ.DeequSchemaFactory
import org.apache.spark.sql.DataFrame

private[dqsuite] case class SchemaCheckRunner(
  context: DQSuiteDatasetContext,
) {
  def run(df: DataFrame): SchemaCheckResult = {
    // Check for missing columns
    val missingRequiredColumns = context.config.schema
      .getOrElse(Seq.empty)
      .filter(_.required)
      .map(_.column)
      .filterNot(df.columns.contains)

    // Check for extra columns
    val schemaColumns = context.config.schema.getOrElse(Seq.empty).map(_.column).toSet
    val extraColumns = df.columns
      .filterNot(schemaColumns.contains)

    val missingColumns = context.config.schema
      .getOrElse(Seq.empty)
      .map(_.column)
      .filterNot(df.columns.contains)

    // Fill missing columns with nulls
    val augmentedDf = missingColumns.foldLeft(df) { (df, col) =>
      df.withColumn(col, F.lit(null).cast("string"))
    }

    val schema = DeequSchemaFactory.buildSeq(context.config)
    val deequResult = RowLevelSchemaValidator
      .validate(augmentedDf, schema)

    SchemaCheckResult(
      missingRequiredColumns,
      extraColumns,
      deequResult,
    )
  }

}

case class SchemaCheckResult(
  missingColumns: Seq[String],
  extraColumns: Seq[String],
  deequResult: RowLevelSchemaValidationResult,
) {
  def validRows: DataFrame = deequResult.validRows
  def numValidRows: Long = deequResult.numValidRows
  def invalidRows: DataFrame = deequResult.invalidRows
  def numInvalidRows: Long = deequResult.numInvalidRows
  def isValid: Boolean = missingColumns.isEmpty && numInvalidRows == 0
}
