package dqsuite.deequ

import com.amazon.deequ.schema.RowLevelSchema
import dqsuite.config.{DecimalColumnConfig, IntColumnConfig, SchemaColumnDefinitionConfig, SchemaExprConfig, SourceConfig, StringColumnConfig, TimestampColumnConfig}
import dqsuite.utils.RuntimeCompileUtils

private[dqsuite] object DeequSchemaFactory {
  def build(
    config: SchemaColumnDefinitionConfig,
  ): RowLevelSchema = {
    config match {
      case col: StringColumnConfig => {
        RowLevelSchema()
          .withStringColumn(col.column, col.isNullable, col.minLength, col.maxLength, col.matches)
      }
      case col: IntColumnConfig => {
        RowLevelSchema()
          .withIntColumn(col.column, col.isNullable, col.minValue, col.maxValue)
      }
      case col: DecimalColumnConfig => {
        RowLevelSchema()
          .withDecimalColumn(col.column, col.precision, col.scale, col.isNullable)
      }
      case col: TimestampColumnConfig => {
        RowLevelSchema()
          .withTimestampColumn(col.column, col.mask, col.isNullable)
      }
      case col: SchemaExprConfig => {

        val expression = col.expression.replace("@", s""""${col.column}"""")
        val source =
          s"""
             |import com.amazon.deequ.schema._
             |RowLevelSchema()
             |${expression}
        """.stripMargin
        RuntimeCompileUtils.evaluate(source).asInstanceOf[RowLevelSchema]
      }
      case _ => throw new Exception("Unsupported schema column type")
    }
  }

  def buildSeq(
    config: SourceConfig,
  ): RowLevelSchema = {
    val columnDefinitions = config.schema
      .getOrElse(Seq.empty)
      .map(DeequSchemaFactory.build)
      .flatMap(_.columnDefinitions)

    RowLevelSchema(columnDefinitions)
  }
}
