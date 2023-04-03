package dqsuite.config

import com.amazon.deequ.checks.CheckLevel
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config._
import org.yaml.snakeyaml.Yaml

import java.io.InputStream

private[dqsuite] case class AnalyzerConfig(column: String, expression: String, where: Option[String], enabled: Boolean)

private[dqsuite] object AnalyzerConfig {
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

private[dqsuite] case class CheckConfig(
  column: String,
  level: CheckLevel.Value,
  name: Option[String],
  expression: String,
  enabled: Boolean
)

private[dqsuite] object CheckConfig {
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

private[dqsuite] case class AnomalyDetectionConfig(
  column: String,
  level: CheckLevel.Value,
  analyser: AnalyzerConfig,
  strategy: String,
  withTags: Map[String, String],
  description: Option[String],
  historyWindow: Option[Long],
  enabled: Boolean,
)

private[dqsuite] object AnomalyDetectionConfig {
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

private[dqsuite] case class SourceConfig(
  tags: Map[String, String] = Map.empty,
  analyzers: Seq[AnalyzerConfig],
  checks: Seq[CheckConfig],
  anomalyDetection: Seq[AnomalyDetectionConfig]
)

private[dqsuite] object SourceConfig {
  implicit val loader: ConfigLoader[SourceConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    SourceConfig(
      c.getOptional[Map[String, String]]("tags").getOrElse(Map.empty),
      c.getSeq[AnalyzerConfig]("analyzers"),
      c.getSeq[CheckConfig]("checks"),
      c.getSeq[AnomalyDetectionConfig]("anomaly_detection")
    )
  }
}

private[dqsuite] case class DQSuiteConfig(sources: Map[String, SourceConfig])

private[dqsuite] object DQSuiteConfig {
  implicit val loader: ConfigLoader[DQSuiteConfig] = (config: Config, path: String) =>
    DQSuiteConfig(
      Configuration(config).getMap[SourceConfig]("sources")
  )

  def loadStream(stream: InputStream): DQSuiteConfig = {
    val yaml = new Yaml()
    val obj: Object = yaml.load(stream)
    val jsonWriter = new ObjectMapper()
    val json = jsonWriter.writeValueAsString(obj)

    val rawConfig = ConfigFactory
      .parseString(json)
      .resolve()

    val config: DQSuiteConfig = Configuration(rawConfig).get("")
    config
  }
}
