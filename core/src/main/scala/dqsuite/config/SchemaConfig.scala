package dqsuite.config

import com.amazon.deequ.schema.RowLevelSchema
import com.typesafe.config.Config

private[dqsuite] sealed trait SchemaColumnDefinitionConfig

private[dqsuite] case class SchemaExprConfig(
  expression: String,
) extends SchemaColumnDefinitionConfig

private[dqsuite] object SchemaExprConfig {
  implicit val loader: ConfigLoader[SchemaExprConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    SchemaExprConfig(
      c.get[String]("expression")
    )
  }
}

private[dqsuite] case class StringColumnConfig(
  column: String,
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
      c.getOptional[Boolean]("is_nullable").getOrElse(true),
      c.getOptional[Int]("min_length"),
      c.getOptional[Int]("max_length"),
      c.getOptional[String]("matches")
    )
  }
}

private[dqsuite] case class IntColumnConfig(
  column: String,
  isNullable: Boolean = true,
  minValue: Option[Int] = None,
  maxValue: Option[Int] = None
) extends SchemaColumnDefinitionConfig

private[dqsuite] object IntColumnConfig {
  implicit val loader: ConfigLoader[IntColumnConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    IntColumnConfig(
      c.get[String]("column"),
      c.getOptional[Boolean]("is_nullable").getOrElse(true),
      c.getOptional[Int]("min_value"),
      c.getOptional[Int]("max_value")
    )
  }
}

private[dqsuite] case class DecimalColumnConfig(
  column: String,
  precision: Int,
  scale: Int,
  isNullable: Boolean = true,
) extends SchemaColumnDefinitionConfig

private[dqsuite] object DecimalColumnConfig {
  implicit val loader: ConfigLoader[DecimalColumnConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    DecimalColumnConfig(
      c.get[String]("column"),
      c.get[Int]("precision"),
      c.get[Int]("scale"),
      c.getOptional[Boolean]("is_nullable").getOrElse(true)
    )
  }
}

private[dqsuite] case class TimestampColumnConfig(
  column: String,
  mask: String,
  isNullable: Boolean = true
) extends SchemaColumnDefinitionConfig

private[dqsuite] object TimestampColumnConfig {
  implicit val loader: ConfigLoader[TimestampColumnConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    TimestampColumnConfig(
      c.get[String]("column"),
      c.get[String]("mask"),
      c.getOptional[Boolean]("is_nullable").getOrElse(true)
    )
  }
}

private[dqsuite] object SchemaColumnDefinitionConfig {
  implicit val loader: ConfigLoader[SchemaColumnDefinitionConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    c.getOptional[String]("type").getOrElse("expr") match {
      case "expr"      => SchemaExprConfig.loader.load(config, path)
      case "string"    => StringColumnConfig.loader.load(config, path)
      case "int"       => IntColumnConfig.loader.load(config, path)
      case "decimal"   => DecimalColumnConfig.loader.load(config, path)
      case "timestamp" => TimestampColumnConfig.loader.load(config, path)
      case _           => throw new RuntimeException("Unknown type")
    }
  }
}
