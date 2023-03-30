package dqsuite.config

import com.amazon.deequ.checks.CheckLevel
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.typesafe.config._
import dqsuite.utils.HdfsUtils
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, File, FileInputStream, InputStream}
import java.time.Instant

case class AnalyzerConfig(column: String, expression: String, where: Option[String], enabled: Boolean)

object AnalyzerConfig {
  implicit val loader: ConfigLoader[AnalyzerConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    AnalyzerConfig(
      c.get[String]("column"),
      c.get[String]("expression"),
      c.getOptional[String]("where"),
      c.getOptional[Boolean]("enabled").getOrElse(true)
    )
  }
}

case class CheckConfig(
  column: String,
  level: CheckLevel.Value,
  name: Option[String],
  expression: String,
  enabled: Boolean
)

object CheckConfig {
  implicit val loader: ConfigLoader[CheckConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    CheckConfig(
      c.get[String]("column"),
      CheckLevel.withName(c.getOptional[String]("level").getOrElse("Error").capitalize),
      c.getOptional[String]("name"),
      c.get[String]("expression"),
      c.getOptional[Boolean]("enabled").getOrElse(true)
    )
  }
}

case class AnomalyDetectionConfig(
  column: String,
  level: CheckLevel.Value,
  analyser: AnalyzerConfig,
  strategy: String,
  withTags: Map[String, String],
  description: Option[String],
  historyWindow: Option[Long],
  enabled: Boolean,
)

object AnomalyDetectionConfig {
  implicit val loader: ConfigLoader[AnomalyDetectionConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    AnomalyDetectionConfig(
      c.get[String]("column"),
      CheckLevel.withName(c.getOptional[String]("level").getOrElse("Error").capitalize),
      AnalyzerConfig.loader.load(config),
      c.get[String]("strategy"),
      c.getOptional[Map[String, String]]("with_tags").getOrElse(Map.empty),
      c.getOptional[String]("description"),
      c.getOptional[Long]("history_window").map(_ * 1000), // Convert to milliseconds
      c.getOptional[Boolean]("enabled").getOrElse(true)
    )
  }
}

case class SourceConfig(
  format: Option[String],
  sparkOptions: Option[Map[String, String]],
  tags: Map[String, String] = Map.empty,
  analyzers: Seq[AnalyzerConfig],
  checks: Seq[CheckConfig],
  anomalyDetection: Seq[AnomalyDetectionConfig]
)

object SourceConfig {
  implicit val loader: ConfigLoader[SourceConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    SourceConfig(
      c.getOptional[String]("format"),
      c.getOptional[Map[String, String]]("spark_options"),
      c.getOptional[Map[String, String]]("tags").getOrElse(Map.empty),
      c.getSeq[AnalyzerConfig]("analyzers"),
      c.getSeq[CheckConfig]("checks"),
      c.getSeq[AnomalyDetectionConfig]("anomaly_detection")
    )
  }
}

case class DataQualityConfig(sources: Map[String, SourceConfig])

object DataQualityConfig {
  implicit val loader: ConfigLoader[DataQualityConfig] = (config: Config, path: String) =>
    DataQualityConfig(
      Configuration(config).getMap[SourceConfig]("sources")
  )

  def loadStream(stream: InputStream): DataQualityConfig = {
    val yamlReader = new ObjectMapper(new YAMLFactory())
    val obj = yamlReader.readValue(stream, classOf[Object])
    val jsonWriter = new ObjectMapper()
    val json = jsonWriter.writeValueAsString(obj)

    val rawConfig = ConfigFactory
      .parseString(json)
      .resolve()

    val config: DataQualityConfig = Configuration(rawConfig).get("")
    config
  }
}
