package dqsuite.config

import com.amazon.deequ.schema.RowLevelSchema
import com.typesafe.config.Config

/** Base trait for all schema configuration types.
  */
private[dqsuite] sealed trait SchemaColumnDefinitionConfig {

  /** @return
    *   The column name.
    */
  def column: String

  /** @return
    *   Whether the column is required to be present in the data or not.
    */
  def required: Boolean

  /** @return
    *   Whether the column values can be null or not.
    */
  def isNullable: Boolean

  /** @return
    *   Rename the column to this name in the output.
    */
  def alias: Option[String]
}

/** Schema check based on a custom deequ [[com.amazon.deequ.schema.RowLevelSchema]] method(s).
  */
private[dqsuite] case class SchemaExprConfig(
  column: String,
  required: Boolean,
  alias: Option[String],
  expression: String,
  isNullable: Boolean = true
) extends SchemaColumnDefinitionConfig

private[dqsuite] object SchemaExprConfig {
  implicit val loader: ConfigLoader[SchemaExprConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    SchemaExprConfig(
      c.get[String]("column"),
      c.getOptional[Boolean]("required").getOrElse(true),
      c.getOptional[String]("alias"),
      c.get[String]("expression")
    )
  }
}

/** Schema check based on deequ [[com.amazon.deequ.schema.StringColumnDefinition]]
  */
private[dqsuite] case class StringColumnConfig(
  column: String,
  required: Boolean,
  alias: Option[String],
  isNullable: Boolean = true,
  minLength: Option[Int] = None,
  maxLength: Option[Int] = None,
  matches: Option[String] = None
) extends SchemaColumnDefinitionConfig

private[dqsuite] object StringColumnConfig {
  implicit val loader: ConfigLoader[StringColumnConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    StringColumnConfig(
      c.get[String]("column"),
      c.getOptional[Boolean]("required").getOrElse(true),
      c.getOptional[String]("alias"),
      c.getOptional[Boolean]("is_nullable").getOrElse(true),
      c.getOptional[Int]("min_length"),
      c.getOptional[Int]("max_length"),
      c.getOptional[String]("matches")
    )
  }
}

/** Schema check based on deequ [[com.amazon.deequ.schema.IntColumnDefinition]]
  */
private[dqsuite] case class IntColumnConfig(
  column: String,
  required: Boolean,
  alias: Option[String],
  isNullable: Boolean = true,
  minValue: Option[Int] = None,
  maxValue: Option[Int] = None
) extends SchemaColumnDefinitionConfig

private[dqsuite] object IntColumnConfig {
  implicit val loader: ConfigLoader[IntColumnConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    IntColumnConfig(
      c.get[String]("column"),
      c.getOptional[Boolean]("required").getOrElse(true),
      c.getOptional[String]("alias"),
      c.getOptional[Boolean]("is_nullable").getOrElse(true),
      c.getOptional[Int]("min_value"),
      c.getOptional[Int]("max_value")
    )
  }
}

/** Schema check based on deequ [[com.amazon.deequ.schema.DecimalColumnDefinition]]
  */
private[dqsuite] case class DecimalColumnConfig(
  column: String,
  required: Boolean,
  alias: Option[String],
  precision: Int,
  scale: Int,
  isNullable: Boolean = true
) extends SchemaColumnDefinitionConfig

private[dqsuite] object DecimalColumnConfig {
  implicit val loader: ConfigLoader[DecimalColumnConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    DecimalColumnConfig(
      c.get[String]("column"),
      c.getOptional[Boolean]("required").getOrElse(true),
      c.getOptional[String]("alias"),
      c.get[Int]("precision"),
      c.get[Int]("scale"),
      c.getOptional[Boolean]("is_nullable").getOrElse(true)
    )
  }
}

/** Schema check based on deequ [[com.amazon.deequ.schema.TimestampColumnDefinition]]
  */
private[dqsuite] case class TimestampColumnConfig(
  column: String,
  required: Boolean,
  alias: Option[String],
  mask: String,
  isNullable: Boolean = true
) extends SchemaColumnDefinitionConfig

private[dqsuite] object TimestampColumnConfig {
  implicit val loader: ConfigLoader[TimestampColumnConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    TimestampColumnConfig(
      c.get[String]("column"),
      c.getOptional[Boolean]("required").getOrElse(true),
      c.getOptional[String]("alias"),
      c.get[String]("mask"),
      c.getOptional[Boolean]("is_nullable").getOrElse(true)
    )
  }
}

private[dqsuite] object SchemaColumnDefinitionConfig {
  implicit val loader: ConfigLoader[SchemaColumnDefinitionConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    c.getOptional[String]("type").getOrElse("expression") match {
      case "expression" => SchemaExprConfig.loader.load(config, path)
      case "string"     => StringColumnConfig.loader.load(config, path)
      case "int"        => IntColumnConfig.loader.load(config, path)
      case "decimal"    => DecimalColumnConfig.loader.load(config, path)
      case "timestamp"  => TimestampColumnConfig.loader.load(config, path)
      case _            => throw new RuntimeException("Unknown type")
    }
  }
}
