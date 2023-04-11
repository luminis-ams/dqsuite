package dqsuite.runners

import org.apache.spark.sql.{functions => F}
import com.amazon.deequ.schema.{RowLevelSchemaValidationResult, RowLevelSchemaValidator}
import dqsuite.DQSuiteDatasetContext
import dqsuite.deequ.DeequSchemaFactory
import org.apache.spark.sql.DataFrame

private[dqsuite] case class SchemaCheckRunner(
  context: DQSuiteDatasetContext,
  emptyStringAsNull: Boolean = true,
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
    var augmentedDf = missingColumns.foldLeft(df) { (df, col) =>
      df.withColumn(col, F.lit(null).cast("string"))
    }

    // Replace empty strings with nulls (necessary when source file format does not support nulls like CSV)
    if (emptyStringAsNull) {
      val nullableColumns = context.config.schema
        .getOrElse(Seq.empty)
        .filter(c => !c.required || c.isNullable)
        .map(_.column)
        .toSet

      augmentedDf = augmentedDf.schema.fields
        .filter(_.dataType.typeName == "string")
        .map(_.name)
        .filter(nullableColumns.contains)
        .foldLeft(augmentedDf) { (df, col) =>
          df.withColumn(col, F.when(F.col(col) === "", null).otherwise(F.col(col)))
        }
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
