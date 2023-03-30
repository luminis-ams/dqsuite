package dqsuite

import com.amazon.deequ.AnomalyCheckConfig
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.metrics.Metric
import dqsuite.config.{AnalyzerConfig, AnomalyDetectionConfig, CheckConfig, SourceConfig}

import java.time.Instant
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object DeequFactory {
  private val toolbox = currentMirror.mkToolBox()

  private def buildAnalyzer(
    config: AnalyzerConfig,
  ): Seq[Analyzer[_, Metric[_]]] = {
    if (!config.enabled) {
      return Seq.empty
    }

    val expression = config.expression.replace("@", s""""${config.column}"""")
    val source = s"""
    |import com.amazon.deequ.analyzers._
    |$expression :: Nil
    |""".stripMargin

    val analyzers = toolbox.eval(toolbox.parse(source)).asInstanceOf[Seq[Analyzer[_, Metric[_]]]]
    analyzers
  }

  def buildAnalyzers(
    config: SourceConfig,
  ): Seq[Analyzer[_, Metric[_]]] = {
    config.analyzers.flatMap(DeequFactory.buildAnalyzer)
  }

  private def buildCheck(
    config: CheckConfig,
  ): Option[Check] = {
    if (!config.enabled) {
      return None
    }

    val level = config.level.toString
    val name = config.name.getOrElse(s"Check failed for ${config.column}")
    val expression = config.expression.replace("@", s""""${config.column}"""")
    val source = s"""
    |import com.amazon.deequ.constraints.ConstrainableDataTypes
    |com.amazon.deequ.checks.Check(com.amazon.deequ.checks.CheckLevel.$level, \"$name\")
    |$expression
    """.stripMargin

    val check = toolbox.eval(toolbox.parse(source)).asInstanceOf[Check]
    Some(check)
  }

  def buildChecks(
    config: SourceConfig,
  ): Seq[Check] = {
    config.checks.flatMap(DeequFactory.buildCheck)
  }

  private def buildAnomalyDetector(
    datasetDate: Option[Instant],
    config: AnomalyDetectionConfig,
    withTags: Map[String, String] = Map.empty,
  ): Option[AnomalyDetectionInstance] = {
    if (!config.enabled) {
      return None
    }

    val expression = config.strategy.replace("@", s""""${config.column}"""")
    val source = s"""
    |import com.amazon.deequ.anomalydetection._
    |$expression
    """.stripMargin
    val strategy = toolbox.eval(toolbox.parse(source)).asInstanceOf[AnomalyDetectionStrategy]

    val analyser = buildAnalyzer(config.analyser).head.asInstanceOf[Analyzer[_ <: State[_], Metric[Double]]]

    val level = CheckLevel.withName(config.level.toString.capitalize)
    val description = config.description.getOrElse(s"Anomaly detected for ${config.column}")

    val dateTo = datasetDate.getOrElse(Instant.now()).toEpochMilli
    val dateFrom = config.historyWindow.map(w => dateTo - w)

    val anomalyCheckConfig = AnomalyCheckConfig(
      level,
      description,
      config.withTags ++ withTags,
      dateFrom,
      dateFrom.map(_ => dateTo),
    )

    Some(AnomalyDetectionInstance(analyser, strategy, anomalyCheckConfig))
  }

  def buildAnomalyDetectors(
    datasetDate: Option[Instant],
    config: SourceConfig,
    withTags: Map[String, String] = Map.empty,
  ): Seq[AnomalyDetectionInstance] = {
    config.anomalyDetection.flatMap(DeequFactory.buildAnomalyDetector(datasetDate, _, config.tags ++ withTags))
  }
}

case class AnomalyDetectionInstance(
  analyser: Analyzer[_ <: State[_], Metric[Double]],
  strategy: AnomalyDetectionStrategy,
  config: AnomalyCheckConfig,
) {}
