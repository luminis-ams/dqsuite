package dqsuite.config

import com.amazon.deequ.checks.CheckLevel
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config._
import org.yaml.snakeyaml.Yaml

import java.io.InputStream

/** Configuration for a Deequ Analyzer
  * @param column
  *   The column the check will be applied to. @ in expression clause will be replaced with the column name
  * @param expression
  *   The analyzer expression to be evaluated. Consists of a scala Sequence of Deequ Analyzers
  * @param where
  *   Optional where clause to filter the data before applying the analyzer
  * @param enabled
  *   Whether the analyzer should be enabled or not
  */
private[dqsuite] case class AnalyzerConfig(
  column: String,
  expression: String,
  where: Option[String],
  enabled: Boolean
)

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

/** Configuration for a Deequ Check
  * @param column
  *   The column the check will be applied to. @ in expression clause will be replaced with the column name
  * @param level
  *   The deequ [[CheckLevel]] to denote severity of the check
  * @param name
  *   Optional name for the check to be displayed in the report
  * @param expression
  *   The check expression to be evaluated. Consists of a scala Sequence of Deequ Check expressions. See
  *   [[com.amazon.deequ.checks.Check]]
  * @param enabled
  *   Whether the check should be enabled or not
  */
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

/** Configuration for a Deequ Anomaly Detection Rule
  * @param column
  *   The column the check will be applied to. @ in expression clause will be replaced with the column name
  * @param level
  *   The deequ [[CheckLevel]] to denote severity of the check
  * @param analyser
  *   The analyzer to be used to generate the metrics for anomaly detection
  * @param strategy
  *   The strategy to be used for anomaly detection. See [[com.amazon.deequ.anomalydetection]]
  * @param withTags
  *   Optional tags to be added to the anomaly detection result
  * @param description
  *   Optional description for the anomaly detection result
  * @param historyWindow
  *   Optional the timestamp denoting start of the metric collection window for anomaly detection. If not provided, the
  *   entire history will be used.
  * @param enabled
  *   Whether the anomaly detection rule should be enabled or not
  */
private[dqsuite] case class AnomalyDetectionConfig(
  column: String,
  level: CheckLevel.Value,
  analyser: AnalyzerConfig,
  strategy: String,
  withTags: Map[String, String],
  description: Option[String],
  historyWindow: Option[Long],
  enabled: Boolean
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

/** Configuration for a testable Data Source
  * @param tags
  *   Optional tags to be added to the produced metrics and reports corresponding to the source
  * @param schema
  *   Optional schema definition for the source
  * @param analyzers
  *   Optional analyzers to be applied to the source
  * @param checks
  *   Optional checks to be applied to the source
  * @param anomalyDetection
  *   Optional anomaly detection rules to be applied to the source
  */
private[dqsuite] case class SourceConfig(
  tags: Map[String, String] = Map.empty,
  schema: Option[Seq[SchemaColumnDefinitionConfig]],
  analyzers: Seq[AnalyzerConfig],
  checks: Seq[CheckConfig],
  anomalyDetection: Seq[AnomalyDetectionConfig]
)

private[dqsuite] object SourceConfig {
  implicit val loader: ConfigLoader[SourceConfig] = (config: Config, path: String) => {
    val c = Configuration(config)

    SourceConfig(
      c.getOptional[Map[String, String]]("tags").getOrElse(Map.empty),
      c.getOptional[Seq[SchemaColumnDefinitionConfig]]("schema"),
      c.getSeq[AnalyzerConfig]("analyzers"),
      c.getSeq[CheckConfig]("checks"),
      c.getSeq[AnomalyDetectionConfig]("anomaly_detection")
    )
  }
}

/** Root configuration for the DQ Suite
  * @param sources
  *   The sources to be used for the DQ Suite
  */
private[dqsuite] case class DQSuiteConfig(sources: Map[String, SourceConfig])

private[dqsuite] object DQSuiteConfig {
  implicit val loader: ConfigLoader[DQSuiteConfig] = (config: Config, path: String) =>
    DQSuiteConfig(
      Configuration(config).getMap[SourceConfig]("sources")
    )

  def loadStream(stream: InputStream): DQSuiteConfig = {
    val yaml        = new Yaml()
    val obj: Object = yaml.load(stream)
    val jsonWriter  = new ObjectMapper()
    val json        = jsonWriter.writeValueAsString(obj)

    val rawConfig = ConfigFactory
      .parseString(json)
      .resolve()

    val config: DQSuiteConfig = Configuration(rawConfig).get("")
    config
  }
}
